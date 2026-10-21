# Gradle Build Modernisation

Analysis of the Java/Gradle build (`build.gradle`, `settings.gradle`, `gradle.properties`, `buildSrc/`) and a staged
plan for modernising it. Work happens on the `build-modernisation` branch. The C/C++ CMake build is out of scope.

Status legend: ☐ not started · ◐ in progress · ☑ done

## Baseline (as of October 2026)

- Gradle 9.8.2 wrapper (`-all` distribution), Groovy DSL.
- 9 modules declared in `settings.gradle`, but **no per-module build files**: everything lives in a ~1,400-line root
  `build.gradle`.
- CI (`.github/workflows/ci.yml`) builds on JDK 17, 21 and 25, using `gradle/actions/setup-gradle`.

### Already modern

- Recent Gradle, `plugins {}` DSL, version catalog (`gradle/libs.versions.toml`), also shared with `buildSrc`.
- Java toolchains, JUnit BOM, `com.gradleup.shadow`.
- Mostly lazy task APIs (`tasks.register`, `configureEach`).
- Custom task types in Java in `buildSrc` (e.g. `LoggingTest.configureLoggingTests`), which is the start of
  convention-style build logic.

## Findings

### 1. Centralised cross-project configuration

All configuration is in the root script using `allprojects {}`, `subprojects {}` and `project(':aeron-x') { … }`.
This is a pre-2019 style. Modern practice is convention plugins (in `buildSrc` or a `build-logic` included build), plus
a short build file per module that applies them.

Several places reach into other projects' model, which blocks Gradle's Isolated Projects feature:

- `javadoc { source += project(':aeron-client').sourceSets.main.allJava }` and similar (most modules).
- `testImplementation project(path: ':aeron-client', configuration: 'tests')` in `aeron-system-tests`.
- `aeron-all` reading other projects' `sourceSets` for its sources and Javadoc jars.

### 2. Duplication

| Block | Copies |
|---|---|
| `publishing { repositories { maven { … } } }` + `signing { … }` | 8 |
| Manifest attributes (`Automatic-Module-Name`, `Implementation-*`) | 6 |
| SBE `SbeTool` `JavaExec` tasks (archive and cluster codecs, C/C++ codecs, versioned test codecs) | 10 |
| `generateConfigDoc` | 4 |
| `validateConfigExpectations` | 2 |
| Event-log system properties on `slowTest`, `bindingsTest`, `topologyTest`, samples and system tests | 5 |

A publishing convention plugin and a typed `SbeGenerate` task would remove several hundred lines.

### 3. Generated sources wired by hand

`aeron-archive` and `aeron-cluster` use:

- `api files('build/classes/java/generated')`, a dependency on a raw output path.
- `testImplementation files("…/classes/java/generatedTest")`.
- Manual task chains: `compileJava.dependsOn 'compileGeneratedJava'`,
  `compileGeneratedJava.dependsOn 'generateCodecs'`, and the same again for the test codecs.

Modern approach: register the generator task's output as a source dir, e.g.
`sourceSets.main.java.srcDir(tasks.named('generateCodecs'))`, so Gradle infers task dependencies. The explicit
`dependsOn` calls, the extra `generated` source set and the `files(...)` dependencies can then go.

### 4. Correctness and configuration-cache issues

- **Compiler arguments are added in `doFirst`** (`subprojects { tasks.withType(JavaCompile) … }`). This covers
  `--release`, `-Xlint`, `-Werror`, `-parameters`, `-Aio.aeron.version` and `-Aio.aeron.gitsha`.
  - Arguments added at execution time are not task inputs. So when only the version or git SHA changes, compilation
    is considered up to date and doesn't rerun, and compiled classes can carry a stale version or git SHA.
  - The closure also uses `java` and `projectDir` at execution time, which the configuration cache doesn't allow.
  - The git SHA is computed with JGit once per compile task (6+ times per build).
- **`buildSrc` tasks call `getProject()` at execution time:** `TutorialPublishTask` and `AsciidoctorPreprocessTask`.
- **Configuration-time values:** many `layout.buildDirectory.get()` string interpolations, and
  `System.getProperty(...)` read during configuration. Lazy providers are preferred.
- **Nothing is switched on in `gradle.properties`:** build cache (`org.gradle.caching`), parallel execution
  (`org.gradle.parallel`) and the configuration cache are all off.

### 5. Test plumbing

- **Hand-built test-jar sharing:** `configurations { tests }` plus `artifacts { tests testJar }`. The standard
  replacement is the `java-test-fixtures` plugin.
- **Repeated `Test` setup:** every `Test` task has `testClassesDirs`/`classpath` overridden. The tag-based tasks
  (`slowTest`, `bindingsTest`, `topologyTest`) repeat their configuration and could become JVM Test Suites
  (`testing.suites`).

### 6. Dependencies and repositories

- Repositories are declared in `allprojects`, including `mavenLocal()`, which makes builds non-reproducible.
  Modern practice is `dependencyResolutionManagement { repositories { … } }` in `settings.gradle` with
  `repositoriesMode = FAIL_ON_PROJECT_REPOS`.
- `failOnVersionConflict()` plus a list of `force` entries is an older pattern. Dependency constraints or `strictly`
  versions say the same thing more precisely.

### 7. Minor

- Groovy DSL rather than Kotlin DSL, which has been Gradle's default since 8.2. Mostly a matter of taste.
- `jar.finalizedBy assembleDist` in `aeron-driver` means every `jar` also builds the zip and tar distributions.
- **Probable bug:** in `copyTestLogs`, `include '/var/coredump/core*'` is matched relative to `from '.'`, so it most
  likely never collects any core dumps.
- The Javadoc footer copyright year is hard-coded (2014-2025).
- The wrapper uses the `-all` distribution; `-bin` is smaller.

## Plan

Each step should keep the existing contract:

- the CI matrix passes, including logging-test variants and tag-based test tasks;
- published artifacts (jars, sources, Javadoc, POMs, driver zip and tar, the `aeron-all` shadow jar) are unchanged.

Compare `publishToMavenLocal` output and POMs before and after each structural step.

| # | Step | Status |
|---|---|---|
| 1 | Move compiler arguments out of `doFirst` so they're real inputs; compute the git SHA once per build; enable build cache and parallel execution. Measure clean and incremental build times before and after. | ◐ |
| 2 | Extract convention plugins (Java defaults, publishing/signing, SBE codec generation, config-doc tasks) into `buildSrc`, and give each module its own build file. | ☐ |
| 3 | Wire generated sources through `srcDir(taskProvider)`; replace the `tests` configuration with `java-test-fixtures`. | ☐ |
| 4 | Make the build configuration-cache safe (injected services in `buildSrc` tasks, lazy providers), then enable the configuration cache. | ☐ |
| 5 | Optional: Kotlin DSL, JVM Test Suites, `settings.gradle` repositories, constraints instead of `force`, and the minor items. | ☐ |

## Log

Record measurements and decisions here as the work progresses.
