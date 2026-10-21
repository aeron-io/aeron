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
  - Arguments added at execution time are not task inputs. When only the git SHA changes, compilation is considered up
    to date, so `AeronVersion.GIT_SHA` in the compiled classes goes stale. A version change *does* trigger
    recompilation, but only by accident: the version is part of the `aeron-annotations` jar filename, which is on the
    annotation processor path.
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
- The `mockitoAgent` configuration is created in every module but never used: no `-javaagent` refers to it.

## Plan

Each step should keep the existing contract:

- the CI matrix passes, including logging-test variants and tag-based test tasks;
- published artifacts (jars, sources, Javadoc, POMs, driver zip and tar, the `aeron-all` shadow jar) are unchanged.

Compare `publishToMavenLocal` output and POMs before and after each structural step.

| # | Step | Status |
|---|---|---|
| 1 | Move compiler arguments out of `doFirst` so they're real inputs; compute the git SHA once per build; enable the build cache. (Parallel execution deferred, see the log.) | ☑ |
| 2 | Extract convention plugins (Java defaults, publishing/signing, SBE codec generation, config-doc tasks) into `buildSrc`, and give each module its own build file. Done in the slices below, one commit each. | ◐ |
| 2a | Safety net: capture the published artifacts before the change, and diff them after each slice. | ☑ |
| 2b | Publishing and signing → `aeron.publishing` convention plugin. | ☑ |
| 2c | Shared Java, test and Checkstyle setup (the `subprojects {}` block) → `aeron.java-conventions`. | ☑ |
| 2d | SBE `JavaExec` tasks → typed `SbeGenerateTask`. | ☑ |
| 2e | `generateConfigDoc` and `validate*Expectations` → `aeron.config-info` convention plugin. | ☑ |
| 2f | Move each `project(':x') {}` block into `x/build.gradle`, one module at a time. | ☐ |
| 3 | Wire generated sources through `srcDir(taskProvider)`; replace the `tests` configuration with `java-test-fixtures`. | ☐ |
| 4 | Make the build configuration-cache safe (injected services in `buildSrc` tasks, lazy providers), then enable the configuration cache. | ☐ |
| 5 | Optional: Kotlin DSL, JVM Test Suites, `settings.gradle` repositories, constraints instead of `force`, and the minor items. | ☐ |

## Log

Record measurements and decisions here as the work progresses.

### Step 1: compiler arguments and build cache

**Bug confirmed before the change.** Starting from a clean tree, an uncommitted edit to `README.md` changes the SHA to
`<sha>+guilty`. `:aeron-client:compileJava` still reported `UP-TO-DATE` and `AeronVersion.GIT_SHA` kept the old
value. After the change, the same edit (tested in a scratch clone with a clean tree) reruns compilation and the
generated class has the `+guilty` SHA.

**Changes made:**

- `JavaCompile` options are set at configuration time rather than in `doFirst`. `--release` now uses
  `options.release`.
- `-Aio.aeron.version` and `-Aio.aeron.gitsha` come from `AeronVersionArgumentProvider`, a
  `CommandLineArgumentProvider` whose `@Input` properties make them real task inputs.
- The git SHA comes from `GitShaValueSource`, a `ValueSource` evaluated once per build rather than once per compile
  task. It is configuration-cache compatible.
- `org.gradle.caching=true` in `gradle.properties`.

**Trade-off (corrected after measuring in step 2):** because the git SHA is now a real input, the first uncommitted
edit in a clean tree (SHA becomes `+guilty`) and every new commit recompile all modules that use the
`aeron-annotations` processor. Most of that cost already existed on `master`:

- `AeronVersion.GIT_SHA` is a `public static final` constant, and a changed constant counts as an API change.
- So recompiling the edited module with the new SHA already forced every module downstream of it to recompile.

The extra cost is recompiling modules upstream of the edit. For an edit in `aeron-client`, that is `aeron-annotations`:
about 1s, measured at 6.3s on `master` and 7.4s on the branch. If even that matters, one option is to embed the real
SHA only for CI or release builds.

**Not changed:** `org.gradle.parallel`. It makes `assemble` faster, but it would also run test tasks from different
modules at the same time, and these tests start media drivers and share Aeron directories and ports. Needs a decision
and a full CI run before enabling, or limiting to non-test tasks.

**Measurements:** `./gradlew <tasks> -x javadoc`, warm daemon, JDK 26, 18-core Mac, wall-clock seconds from two runs
each.

| Scenario | Before | After |
|---|---|---|
| `clean assemble`, no build cache | 10.8 / 9.5 | 9.2 / 9.1 |
| `clean assemble`, build cache hit | n/a | 5.1 / 4.9 |
| `clean assemble`, no build cache, `--parallel` | not measured | 6.9 / 5.7 |
| No-op `assemble` | 1.5 | 1.3 |
| Comment appended to `Aeron.java`, then `assemble` | 6.6 | 1.7 |

The last row isn't a like-for-like comparison. The "before" figure was a first edit after a commit, which recompiles
downstream modules because of the SHA constant (explained in the trade-off above). The "after" figure was taken with
the tree already dirty. See the step 2 performance check for a like-for-like comparison. Verified with `compileTestJava`, `checkstyleMain`, `checkstyleTest` and `:aeron-client:test`, all passing.
The full CI matrix (JDK 17, 21, 25) has not been run.

### Step 2a: artifact comparison safety net

Every structural slice in step 2 must leave the published output unchanged. The check:

1. Publish to a throwaway local repository rather than `~/.m2`. The build has `mavenLocal()` on its repository list,
   so publishing to `~/.m2` could affect later dependency resolution.

   ```
   ./gradlew clean publishToMavenLocal -Dmaven.repo.local=<dir>
   ```

2. Build the baseline at the slice's parent commit, with the slice's changes stashed and a trivial uncommitted edit
   (e.g. a space appended to `README.md`). Both builds then embed the same git SHA (`<parent>+guilty`).
   - The SHA appears in every `*Version` class, and the compiler copies those string constants into the classes that
     use them (e.g. `ConsensusModule$Context`, `SystemCounterDescriptor`, `TestNode`).
   - So builds with different SHAs differ in dozens of classes, and excluding them all would hide real changes.
3. Diff the two repositories. The diff covers:
   - every published file (9 publications, 41 files, ignoring `maven-metadata-local.xml`);
   - jar and zip entries by name and CRC, and tar entries by name and size;
   - POMs as text;
   - Gradle `.module` files as JSON, ignoring per-file checksums and sizes.

Checked on an unchanged rebuild: 0 differences, so the output is stable between builds and any difference means the
build changed. The comparison script is not committed. It is a one-off migration aid, and can be added under
`buildSrc/` if reviewers want to rerun it.

Publishing to a local repository doesn't exercise the release path. So for slices that touch publishing, also compare:

- the publishing repository URL and whether credentials are set, for each project;
- the publication names and `Sign` tasks (dumped by an init script);
- the output of `tasks --all` for publish, sign and upload tasks.

Run these comparisons three ways: a snapshot version without credentials, a snapshot version with credentials, and a
release version (`version.txt` set to `1.54.0`) with credentials.

### Step 2b: `aeron.publishing` convention plugin

`buildSrc/src/main/groovy/aeron.publishing.gradle` is a precompiled Groovy script plugin. `buildSrc` now applies
`groovy-gradle-plugin`. The plugin:

- applies `maven-publish` and `signing`;
- sets the shared POM on every `MavenPublication`;
- configures the Sonatype Central Portal repository, choosing the snapshot or release URL from the project version;
- signs all publications, and keeps the existing "only sign release versions when `publish` is in the task graph"
  rule.

Each module still declares its own publication, so publication and task names are unchanged (e.g.
`publishAeronClientPublicationToMavenRepository`). The root `build.gradle` lost 228 lines: the POM closure, the
repository URL and signing variables, the `Sign` rule, and 8 copies each of the repository and signing blocks.

The Sonatype credentials and `isReleaseVersion` stay in the root script for now, because the root
`uploadArtifactsToCentralPortal` task uses them. `getConfigProperty` is duplicated between the root and the plugin
until that task moves.

Verified:

- artifacts: 0 differences against a same-SHA baseline at `d54a6926e5`;
- publishing repositories, credentials, publications, `Sign` tasks and the `tasks --all` listing: identical in all
  three scenarios (snapshot, snapshot with credentials, release with credentials).

Not verified: an actual upload to Sonatype or a real signing run, because that needs credentials and a GPG key.

### Step 2c: `aeron.java-conventions` convention plugin

`buildSrc/src/main/groovy/aeron.java-conventions.gradle` now holds what was the body of the root `subprojects {}`
block:

- the `base`, `java-library` and `checkstyle` plugins;
- the toolchain, release level and compiler options;
- the shared test dependencies;
- all `Test` task configuration, including `slowTest`, `bindingsTest` and `topologyTest`;
- the Javadoc options;
- the `testJar` and `tests` configuration.

The root `subprojects {}` block now only sets `group` and `version` and applies the plugin. Supporting changes:

- **Java version parsing** moved into `BuildJavaVersion` in `buildSrc`. The root script still uses it for the
  `JavaExec` launcher in `allprojects {}`.
- **Git SHA:** the plugin creates its own `GitShaValueSource` provider, pointed at `layout.settingsDirectory`. This
  turned out to compute the SHA once per project rather than once per build; fixed after 2d, see the performance
  check.
- **Version catalog:** the plugin reads it through `VersionCatalogsExtension`. While the plugin is applied from the
  root `subprojects {}` block, the subprojects don't have the catalog extension yet, so it falls back to the root
  project's catalog. This fallback goes away in 2f.
- **Unchanged on purpose:** the unused `mockitoAgent` configuration and the execution-time `doFirst` in `javadoc`.
  This slice preserves behaviour; those belong to step 4 and step 5.

Verification adds a second check alongside the artifact diff: an init script that registers a `dumpConfig` task in
every project. It prints about 2,500 facts covering:

- for compile, test, Javadoc, jar and `JavaExec` tasks: options, JVM arguments, system properties, JUnit tags,
  classpaths and toolchains;
- for each configuration: its dependencies and artifacts;
- each project's plugins and task names.

Results:

- **Configuration:** identical to the parent commit, apart from each module's plugin list now including the new
  plugin.
- **Artifacts:** 0 differences.
- **Checks:** `checkstyleMain`, `checkstyleTest`, `:aeron-client:test` and `:aeron-driver:loggingTest` pass.

### Step 2d: typed `SbeGenerateTask`

`buildSrc/src/main/java/io/aeron/build/SbeGenerateTask.java` extends `JavaExec` and runs `SbeTool`. Its typed
properties are:

- `schemas`, `validationXsd` and `outputDir`;
- `targetLanguage`;
- optionally, `targetNamespace` and `schemaTransformVersion`.

Each of the 10 registrations is now 6–8 lines instead of 15–17, and `build.gradle` is 66 lines shorter. Because the
task is still a `JavaExec`, the root `allprojects { tasks.withType(JavaExec) … }` toolchain launcher still applies.

Behaviour changes, both deliberate:

- **Bug fix:** cluster `generateCodecs` didn't declare `aeron-cluster-node-state-codecs.xml` as an input, so editing
  that schema didn't regenerate its codecs. Every schema passed to SBE is now an input. Verified: editing the
  node-state schema reruns the task.
- **`--add-opens` removed from code generation:** 7 of the 10 tasks passed
  `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED`, but 3 never did (cluster `generateCodecs`,
  `generateTestCodecsV5` and `generateTestCodecsV12`), so SbeTool evidently doesn't need it. All 10 generators,
  including C and C++, now run without it and produce byte-identical output with no warnings. Tested on JDK 26 only;
  CI will cover 17, 21 and 25.

The flag remains everywhere it is a runtime requirement of Aeron, because Agrona's `UnsafeApi` uses
`jdk.internal.misc.Unsafe` (see `CHANGELOG.adoc`):

- the test JVMs, in `aeron.java-conventions`;
- the sample scripts;
- the C/C++ test harnesses that launch Java drivers and archives.

Removing it there would need Agrona to change how it accesses `Unsafe`, which is outside the scope of this work.

Verified:

- all 761 generated files are byte-identical to the parent commit: archive and cluster main and test codecs, plus the
  archive C and C++ codecs written via `-Dcodec.target.dir`;
- artifacts: 0 differences; configuration dump: identical;
- archive and cluster `compileTestJava` and Checkstyle pass.

Left for step 3:

- **Overlapping outputs:** archive and cluster each have three test-codec tasks writing into one `generated-test`
  directory, which Gradle can't track per task.
- **Shared target directory:** setting `-Dcodec.target.dir` sends the C and the C++ codecs to the same directory.

### Performance check after 2d

**Regression found and fixed.** No-op builds had become about 0.3s slower than `master`.

- Cause: since 2c, each project created its own `GitShaValueSource` provider, and Gradle caches a `ValueSource`
  value per provider instance. So the SHA was computed 8 times per build, about 30ms each with JGit.
- Fix: `GitShaValueSource` is replaced by `GitShaService`, a shared build service that computes the SHA once per
  build, registered with `registerIfAbsent`. Verified:
  - one computation per build, and no warnings;
  - the SHA is still a compile input: a clean tree gives `<sha>`, and the first edit recompiles with `<sha>+guilty`.

**Method:**

- Both sides were measured back to back in fresh clones in the scratchpad, with a clean working tree: `master`
  (`5bc29c38af`), and this branch at `cd18c975b8` plus the build service fix.
- Each side had a warm-up build first. Figures are 3 runs and the median, in seconds, from `./gradlew <tasks>
  -x javadoc` on JDK 26 and an 18-core Mac.
- Edit scenarios change a method body, build, then revert. The first run is a first edit after a commit.

| Scenario | `master` runs | branch runs | `master` median | branch median |
|---|---|---|---|---|
| Configuration only (`help`) | 1.12 / 0.92 / 0.81 | 0.76 / 0.74 / 0.75 | 0.92 | 0.75 |
| `clean assemble`, no build cache | 8.59 / 8.65 / 8.81 | 8.13 / 8.09 / 8.40 | 8.65 | 8.13 |
| `clean assemble`, build cache | 8.67 / 8.85 / 8.63 (cache off on `master`) | 4.92 / 4.83 / 5.25 | 8.67 | 4.92 |
| No-op `assemble` | 1.35 / 1.25 / 1.26 | 1.37 / 1.29 / 1.29 | 1.26 | 1.29 |
| Edit method body in `aeron-client`, `assemble` | 6.34 / 1.32 / 1.23 | 7.44 / 1.39 / 1.31 | 1.32 | 1.39 |
| Edit in `aeron-cluster`, `assemble` | 3.11 / 1.27 / 1.24 | 3.21 / 1.33 / 1.33 | 1.27 | 1.33 |
| `clean assemble`, no build cache, `--parallel` | 5.99 / 5.53 / 5.57 | 5.18 / 5.25 / 5.83 | 5.57 | 5.25 |

Reading the table:

- **Build cache:** the clear win so far. A clean build is about 43% faster when the cache has the outputs.
- **Everything else is level.** Configuration, clean builds, no-op builds and incremental builds are within noise of
  `master`, so the restructuring in step 2 hasn't cost anything.
- **First edit after a commit:** recompiles downstream modules on both sides. On `master` a client edit recompiled 6
  modules; on the branch it recompiles 7, adding `aeron-annotations`. That is the +1.1s in the first client-edit run.
- **Not measured yet:** the configuration cache (step 4) and parallel execution (deferred). Those are where the next
  real gains are.

### Step 2e: `aeron.config-info` convention plugin

`buildSrc/src/main/groovy/aeron.config-info.gradle` is applied by client, driver, archive and cluster. It provides:

- **`generateConfigDoc`**, registered in every module that applies the plugin.
- **The `aeronConfigInfo` extension** (`AeronConfigInfoExtension`), for opting into C source validation:
  - client calls `validateConfigExpectations()` and `validateCounterExpectations()`;
  - driver calls `validateConfigExpectations()`.
- **`FilePathArgumentProvider`**, which passes file locations to the tools as absolute paths, resolved only when the
  task runs.

The root `build.gradle` lost 81 lines (7 registrations).

Behaviour changes, both deliberate:

- **Bug fix:** the tasks read the `.dat` files written by the annotation processors during `compileJava`, but didn't
  depend on it. On a clean build, `./gradlew :aeron-client:generateConfigDoc` failed with `NoSuchFileException`. The
  file locations are now derived from `compileJava`'s header output directory, so Gradle runs compilation first.
- **Classpath:** the tool classpath is now a `configInfoTool` configuration depending on `project(':aeron-annotations')`.
  Previously it reached into that project's `sourceSets.main.runtimeClasspath`, a cross-project access that blocks
  Isolated Projects.

Paths in the validation output are now absolute (e.g. `<root>/aeron-client/src/main/c`) rather than relative to the
module. The validations also declare the C sources as inputs. That doesn't affect up-to-date checks, because they have
no outputs and still run every time.

Verified:

- run from clean, without compiling first, all 7 tasks succeed;
- all 4 generated config docs are byte-identical to the parent commit;
- the validation output is identical apart from the absolute paths (244 lines);
- on a second run, `generateConfigDoc` is `UP-TO-DATE`;
- configuration dump: the only differences are the new plugin and the new `configInfoTool` configuration; task names
  are unchanged;
- artifacts: 0 differences.

**Observation, not changed:** the validation tasks report missing C `#define`s (e.g. for
`AERON_THREAD_AFFINITY_USE_CPUSET_OFFSETS` in the client) but exit 0, so they never fail the build. They aren't run by
CI either. Worth asking the team whether these should become real checks.
