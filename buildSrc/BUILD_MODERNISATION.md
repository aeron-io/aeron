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
| 2 | Extract convention plugins (Java defaults, publishing/signing, SBE codec generation, config-doc tasks) into `buildSrc`, and give each module its own build file. Done in the slices below, one commit each. | ☑ |
| 2a | Safety net: capture the published artifacts before the change, and diff them after each slice. | ☑ |
| 2b | Publishing and signing → `aeron.publishing` convention plugin. | ☑ |
| 2c | Shared Java, test and Checkstyle setup (the `subprojects {}` block) → `aeron.java-conventions`. | ☑ |
| 2d | SBE `JavaExec` tasks → typed `SbeGenerateTask`. | ☑ |
| 2e | `generateConfigDoc` and `validate*Expectations` → `aeron.config-info` convention plugin. | ☑ |
| 2f | Move each `project(':x') {}` block into `x/build.gradle`, one module at a time. | ☑ |
| 3 | Wire generated sources and test sharing properly, and remove the remaining cross-project reach-ins. Done in the slices below, one commit each. | ☑ |
| 3a | Main generated codecs (archive, cluster): register `generateCodecs` output as a `main` source dir. Then remove the `generated` source set, `api files('build/classes/...')`, the manual `dependsOn` chains and the custom `sourcesJar`. Exclude generated code from Checkstyle. Update the consumers (cluster, samples, `aeron-all`). Later revised to option B (codecs compiled in their own source set, wired through outputs); see the decisions. | ☑ |
| 3b | Test codecs: give each generator its own output dir (fixes the overlapping outputs) and wire them into the `test` source set. Then remove `generatedTest` and its `files(...)` dependency. | ☑ |
| 3c | Test sharing, **build-only** (see the decision below): drop the unused client `tests` dependency, and replace the `testJar`/`tests` setup in every module with a properly declared one in `aeron-archive` only. | ☑ |
| 3d | Remaining cross-project reach-ins: Javadoc and `aeron-all` sources aggregation move to proper dependency resolution. | ☑ |
| 3e | Apply `aeron.java-conventions` from each module's `plugins {}` block, which removes the version catalog fallback and the root `subprojects*.test` reach-in. | ☑ |
| 4 | Make the build configuration-cache safe, then enable the configuration cache. Done in the slices below, one commit each. | ◐ |
| 4a | Javadoc offline links: replace the `doFirst` that resolves `javadocLinks` and copies files at execution time with a typed `ExtractJavadocLinksTask`. | ☑ |
| 4b | `TutorialPublishTask`: stop calling `getProject()` in its task action. | ☑ |
| 4c | Check that values which must invalidate the cache really do (git SHA, `version.txt`, system properties, environment variables), and fix any that don't. | ☑ |
| 4d | Enable the configuration cache in `gradle.properties`, and measure. | ☐ |
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
  *Correction:* not a real problem. CMake (`aeron-archive/src/main/c/CMakeLists.txt`) invokes `generateCCodecs` on its
  own, with its own target directory. The two only collide if someone passes the property to both tasks in one
  command.

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

### Step 2f: a build file per module

Each module now has its own `build.gradle`, holding what was its `project(':x') {}` block in the root script. The root
`build.gradle` is down from about 1,400 lines at the start to 146. It keeps only build-wide concerns:

- repositories and dependency resolution rules;
- `group`, `version`, and applying `aeron.java-conventions` to subprojects;
- test report and test-log collection;
- `dependencyUpdates` rules;
- the Central Portal upload task.

How the move was done: the same script handled every module. For each one it:

- turned the block's `apply plugin:` lines into a `plugins {}` block;
- replaced `aeronVersion` with `project.version`;
- added the `buildSrc` imports the module uses;
- added `evaluationDependsOn` wherever the module reads another project's `generated` codec source set (cluster and
  samples read archive's; `aeron-all` reads archive's and cluster's). That source set is created by the owning
  project's build file, so it only exists once that project has been evaluated. The root script used to provide the
  ordering implicitly.

Modules were moved one at a time, with the configuration dump checked after each:

- annotations, test-support, client and driver: identical after each move;
- archive: the build failed until its consumers (cluster, samples, `aeron-all`) had also moved, because their
  blocks were still in the root script and the root is evaluated first;
- after the rest moved: identical.

Verified:

- configuration dump: all 2,544 facts identical, including per-project task names, jar manifests and test settings;
- artifacts: 0 differences;
- `assemble`, Checkstyle, `compileTestJava`, `:aeron-client:test` and `:aeron-archive:loggingTest` pass.

The `tasks --all` listing was also compared, but turned out to be unreliable: two runs of the same commit gave
different listings, because its text format doesn't parse cleanly. The task names in the configuration dump come
straight from each project's task container, so they are the reliable comparison.

Still to do, in later steps:

- **`aeron.java-conventions` is still applied from the root `subprojects {}` block.** Moving it into each module's
  `plugins {}` block would let the version catalog fallback in the plugin go. But the root `testReport` and the
  cross-project Javadoc and sources wiring depend on it being applied early, so it belongs with step 3.
- **The `evaluationDependsOn` calls are a symptom of reaching into other projects' source sets** (Javadoc and sources
  aggregation in `aeron-all`, cluster and samples). Step 3 should replace those reach-ins with proper
  configurations, and then the `evaluationDependsOn` calls can go.

### Performance check after step 2

Same method as the check after 2d:

- Fresh clones of `master` (`5bc29c38af`) and the branch (`4ad0282609`), each with a clean working tree, measured back
  to back after a warm-up build.
- 3 runs per scenario; figures are seconds, with the median after the runs.
- JDK 26, 18-core Mac.

| Scenario | `master` | branch |
|---|---|---|
| Configuration only (`help`) | 0.98 / 0.81 / 0.80 → 0.81 | 0.79 / 0.78 / 0.77 → 0.78 |
| `clean assemble -x javadoc`, no build cache | 9.18 / 8.87 / 8.72 → 8.87 | 8.10 / 8.12 / 8.11 → 8.11 |
| `clean assemble -x javadoc`, build cache | 8.66 / 8.48 / 8.55 → 8.55 (cache off) | 4.63 / 4.64 / 4.64 → **4.64** |
| No-op `assemble -x javadoc` | 1.36 / 1.28 / 1.23 → 1.28 | 1.25 / 1.27 / 1.28 → 1.27 |
| Method body edit in `aeron-client`, `assemble -x javadoc` | 6.20 / 1.34 / 1.24 → 1.34 | 7.10 / 1.32 / 1.26 → 1.32 |
| Edit in `aeron-cluster`, `assemble -x javadoc` | 3.10 / 1.23 / 1.22 → 1.23 | 3.15 / 1.29 / 1.26 → 1.29 |
| `clean assemble -x javadoc`, no build cache, `--parallel` | 5.45 / 5.51 / 5.46 → 5.46 | 5.18 / 5.12 / 5.26 → 5.18 |
| `clean javadoc`, no build cache | 21.55 / 21.50 / 20.95 → 21.50 | 21.94 / 21.23 / 20.99 → 21.23 |
| `clean publishToMavenLocal`, no build cache | 24.31 / 24.60 / 25.33 → 24.60 | 26.07 / 25.21 / 24.55 → 25.21 |
| Change to compiled code in `buildSrc`, then `help` | 2.96 / 2.69 / 2.55 → 2.69 | 1.47 / 1.38 / 1.42 → **1.42** |

Reading the table:

- **Build cache:** a clean build is about 46% faster when the cache has the outputs (8.55s to 4.64s).
- **Editing build logic:** about 47% faster. After a `buildSrc` change Gradle recompiles every build script. That is
  one 1,400-line script on `master`, and 10 small scripts plus 3 precompiled plugins on the branch. The likely
  explanation is that one huge script compiles more slowly, but this hasn't been profiled.
- **Everything else** (configuration, clean builds without the cache, no-op and incremental builds, Javadoc,
  publishing) is within run-to-run noise of `master`. The `publishToMavenLocal` median is 0.6s higher, but the runs
  overlap.
- **First edit after a commit:** about 1s slower on the branch for a client edit, as explained in the step 1
  trade-off. `aeron-annotations` is recompiled as well as the downstream modules.

Not covered: test execution time. The tests themselves are unchanged and their configuration was verified identical
in 2c and 2f, so any difference would be noise from the tests rather than the build.

A comment-only change in `buildSrc` was tried first, but it doesn't change the compiled classes, so Gradle doesn't
recompile the build scripts and the scenario measured nothing. The scenario above adds a new method with a unique name
on each run, so a cached script compilation can't be reused.

### Step 3b: versioned test codecs are part of `test`

The test-codec generators are archive's `generateTestCodecsV6`, `generateTestCodecsMarkFileV0` and
`generateTestCodecsMarkFileV1`, and cluster's `generateTestCodecsMarkFileV0`, `generateTestCodecsV5` and
`generateTestCodecsV12`.

- **Separate output directories:** each generator now writes to its own `build/generated-test/<taskName>`. Previously
  all three in each module shared one directory, so Gradle couldn't tell which files belonged to which task
  (overlapping outputs).
- **Wiring:** each output is added to `test` with `sourceSets.test.java.srcDir(tasks.named(...))`.
- **Removed:** the `generatedTest` source set and its tasks, `testImplementation files(".../generatedTest")`, and the
  manual `compileTestJava.dependsOn` chains.

There's no per-edit compile cost, unlike 3a. Test compilation has no annotation processors, so it stays incremental:
an edit to one test reports "Incremental compilation of 1 classes".

Side effect: archive's test jar (consumed by `aeron-system-tests` via the `tests` configuration) is built from `test`
output, so it now also contains the 117 versioned codec classes. Their packages (`io.aeron.archive.codecs.v6`,
`io.aeron.archive.codecs.mark.v0` and `.v1`) exist nowhere else, so nothing clashes; the system tests compile. 3c will
revisit the test jar anyway.

Verified:

- **Clean build:** from clean, `compileTestJava` in archive and cluster runs all six generators via inferred
  dependencies.
- **Generated code:** the test codecs are byte-identical to the 2d baseline (117 files in archive, 261 in cluster),
  with no file produced by more than one task.
- **Tests:** the tests that use the versioned codecs pass: `ArchiveMarkFileTest`, `ControlSessionAdapterV6Test`,
  `ClusterMarkFileTest` and both modules' `SessionEventCodecCompatibilityTest`. That's 53 tests, 0 failures, and 4
  skips declared in the tests themselves.
- **Configuration:** the only differences are:
  - the `generatedTest` source set's entries are gone;
  - the manual `dependsOn` and `files(...)` dependency are gone;
  - the `build/classes/java/generatedTest` directories have gone from 16 test classpaths.
- **Other checks:** `:aeron-system-tests:compileTestJava` and `checkstyleTest` pass. Published artifacts are
  unaffected, because test classes aren't published.

### Step 3c: test jar sharing, build-only

Previously the conventions plugin built a `testJar` and a `tests` configuration in all 9 modules. Only
`aeron-system-tests` consumed them, from client and archive. Now:

- **Client:** the system tests no longer depend on client's test jar. None of client's test classes are used by the
  system tests (checked including same-package references), and no module has test resources, so nothing could be
  picked up at runtime either.
- **Conventions plugin:** `testJar` and the `tests` configuration are removed from `aeron.java-conventions`.
- **Archive:** `aeron-archive` declares its own `testJar` and a `configurations.consumable('tests')` exposing it. The
  system tests keep using `project(path: ':aeron-archive', configuration: 'tests')`. The explicit
  `dependsOn 'testClasses'` is gone, because `from sourceSets.test.output` carries that dependency.
- **Bug fix:** the classifier was `"test-${base.archivesName}"`, which puts the property object rather than its value
  into the string. The jar was literally named
  `aeron-archive-1.54.0-SNAPSHOT-test-extension 'base' property 'archivesName'.jar`. It is now
  `aeron-archive-1.54.0-SNAPSHOT-tests.jar`.

Verified:

- **Configuration:** the only differences are:
  - `testJar` and `tests` are removed from 8 modules;
  - archive's test jar has the new name;
  - the system tests' classpaths no longer have client's test jar;
  - the system tests' `testImplementation` no longer lists client's `tests`.
- **Tests:** `:aeron-system-tests:test --tests io.aeron.archive.ArchiveSystemTest`, which uses `ArchiveTests`,
  `FailControlResponseListener` and `FailRecordingEventsListener`, passes: 9 tests, 0 failures.
- **Artifacts:** published artifacts are unaffected, because test jars aren't published.

### Step 3d: Javadoc and sources aggregation without reaching into other projects

Previously, client, driver, archive, cluster, samples and `aeron-all` built their Javadoc with
`source += project(':x').sourceSets.main.allJava`, and `aeron-all`'s sources jar read the other projects'
`sourceSets.main.allSource`. Both read another project's model directly, which is what blocks Isolated Projects.

They now resolve the `mainSourceElements` variant that the `java` plugin creates for every project. It lists the
directories of the project's `main` source set, and has the attributes category `verification`, bundling `external`
and verification type `main-sources`.

- **`MainSourcesVariant`** (in `buildSrc`) applies those attributes to a resolvable configuration and makes it
  non-transitive. A module lists exactly the projects it wants, as before; transitive resolution would also follow
  external libraries such as HdrHistogram, which have no such variant.
- **`aeron.java-conventions`** adds a `javadocSource` dependency scope and a resolvable `javadocSourcePath`. The
  `javadoc` task takes the `.java` files from it. Modules declare `javadocSource project(':x')` in place of the old
  `source +=` lines.
- **`aeron-all`** has a `bundledSource`/`bundledSourcePath` pair for its sources jar. The jar's duplicated
  `duplicatesStrategy` line and the unused `project(...)` variables are removed.
- **Workaround for a Gradle limitation:** `mainSourceElements` lists the source directories but not the tasks that
  produce them. Without a fix, `aeron-all:sourcesJar` failed validation with "uses this output of task
  ':aeron-archive:generateCodecs' without declaring an explicit or implicit dependency". The conventions plugin now
  marks every `mainSourceElements` artifact as built by `sourceSets.main.allSource.sourceDirectories`, which carries
  the `generateCodecs` dependency in archive and cluster, and nothing elsewhere.

No `project(':x').sourceSets` reach-ins remain in any build file.

Verified:

- **Artifacts:** 0 differences from a clean build, including all Javadoc jars and the `aeron-all` sources jar.
- **Javadoc:** the source file count is identical for every module (`aeron-all` 530, cluster 527, samples 427, archive
  424, driver 343, client 187, annotations 32).
- **Configuration:** the only differences are the new configurations, whose dependencies list exactly the projects
  the old `source +=` lines named.
- **Task dependencies:** inferred through the variant. A clean `publishToMavenLocal` passes Gradle's
  implicit-dependency validation.

### Step 3e: each module applies its conventions; test report without reach-ins

- **Conventions per module:** every module's `plugins {}` block now starts with `id 'aeron.java-conventions'`, so
  plugins are applied in the same order as before. The root `subprojects {}` block only sets `group` and `version`.
- **Catalog fallback removed:** a module's own build file applies the plugin, so its version catalog extension
  already exists, and the plugin no longer reads the root project's catalog.
- **Test report:** the root `testReport` task is still the same `TestReport` task, with the same output directory.
  Only its input changed. Instead of `testResults.setFrom(subprojects*.test)`, which reached into every subproject's
  tasks, it resolves each subproject's `testResultsElementsForTest` variant: the binary results of its `test` task,
  which the `java` plugin publishes with the attributes category `verification`, test suite name `test` and
  verification type `test-results`. `TestResultsVariant`, alongside `MainSourcesVariant`, applies those attributes
  and makes the resolution non-transitive.

  Gradle's `test-report-aggregation` plugin was tried first, but rejected to keep the build as close as possible to
  the original. It added a `testAggregateTestReport` task, and made `testReport` also run the root project's own
  (empty) test tasks.

No build file reads another project's model any more. The only `rootProject` reference left is the root script
reading its own catalog.

Verified:

- **Configuration:** all 9 modules' configuration dumps are identical, including plugin lists. The root only gains
  the two configurations `subprojectTestResults` and `subprojectTestResultsPath`; its plugins and tasks are unchanged.
- **Task graph:** `testReport` (dry run) triggers exactly the same 84 tasks as before, including all 9 module `test`
  tasks.
- **Report contents:** generated from clean, with the slow modules' `test` tasks excluded, for both the parent commit
  and this change. The report has the same 10 pages and the same headline figures (18 tests, 0 failures, 5 skipped).
  With timings normalised, every page is identical apart from the generation timestamp, and the non-HTML files are
  identical.
- **Artifacts:** 0 differences.
- **Builds and tests:** `assemble`, Checkstyle, `compileTestJava` and `:aeron-client:test` pass.

### Performance check after step 3

Same method as after step 2. Fresh clones of `master` (`5bc29c38af`) and the branch (`ea4e5f595a`), each with a
clean working tree, measured back to back after a warm-up build. Seconds, median of 3 runs unless stated; JDK 26,
18-core Mac.

| Scenario | `master` | branch |
|---|---|---|
| Configuration only (`help`) | 0.98 | 0.79 |
| `clean assemble -x javadoc`, no build cache | 9.25 | 8.03 |
| `clean assemble -x javadoc`, build cache | 8.73 (cache off) | **4.80** |
| No-op `assemble -x javadoc` | 1.28 | 1.27 |
| Method body edit in `aeron-client`, `assemble -x javadoc` | 1.32 | 1.35 |
| Edit in `aeron-cluster`, `assemble -x javadoc` | 1.26 | 1.36 |
| `clean assemble -x javadoc`, no build cache, `--parallel` (6 runs, middle two) | 5.46 / 5.52 | **5.82 / 5.91** |
| `clean javadoc`, no build cache | 21.37 | 21.98 |
| `clean publishToMavenLocal`, no build cache | 24.67 | 23.88 |
| Change to compiled code in `buildSrc`, then `help` | 2.61 | **1.41** |

Reading the table:

- **Still faster:** the build cache (about 45% faster on a clean build when the cache has the outputs) and changes
  to build logic (about 46% faster).
- **Cluster edits:** about 0.1s slower, which is the per-edit cost of step 3a seen through a full `assemble`.
- **New regression, parallel clean builds:** about 0.35s slower than `master`, where after step 2 the branch was
  about 0.3s faster. Timing the commits either side of 3a pins it down: 6 runs each give 5.0–5.5s just before 3a
  (`28ceb081e6`) and 5.6–5.8s just after (`7c58114e8a`).

  Gradle's `--profile` report shows why:

  | Task | Before 3a | After 3a |
  |---|---|---|
  | `:aeron-archive:compileJava` | 0.32s | 0.53s |
  | `:aeron-cluster:compileJava` | 0.41s | 0.69s |
  | `compileGeneratedJava` | 0.32–0.38s each, separate tasks | (removed) |

  Before 3a, the separate `compileGeneratedJava` tasks needed only `generateCodecs`, so with `--parallel` they ran
  alongside the client and driver compiles. Now the codecs are compiled inside `compileJava`, which sits on the
  client → driver → archive → cluster chain, adding about 0.5s to it. Serial builds do the same total work, so
  they're unaffected. CI doesn't use `--parallel` today.
- **Everything else** is within noise of `master`.

This changes the 3a trade-off, so question 1 for the engineers has been updated. Option B (keep the codecs in a
separate compile task, wired through source set outputs) would avoid both 3a costs; option C (incremental processors)
only the per-edit one.

### Option B prototype: codecs compiled separately, wired through outputs

Branch `build-modernisation-codecs-b`, created from `build-modernisation` at `583c2d33e6`.

Archive and cluster bring back a `generated` source set, but wired the modern way:

- `java.srcDir(tasks.named('generateCodecs'))`, so Gradle infers the task dependency;
- `api files(sourceSets.generated.output)` instead of the original raw `api files('build/classes/java/generated')`,
  so the dependency carries the compile task;
- `jar { from sourceSets.generated.output }` and `sourcesJar { from sourceSets.generated.allSource }`;
- the generated folder is added to the module's `mainSourceElements` variant, so the Javadoc and `aeron-all` sources
  aggregation from 3d still includes the codecs.

There are no manual `dependsOn` calls, no hand-written `sourcesJar` task and no `evaluationDependsOn`. Task names
match the original build (`compileGeneratedJava`, `checkstyleGenerated`).

Verified against option A:

- **Artifacts:** 0 differences, including the sources jars and `aeron-all`'s bundled sources.
- **Javadoc:** source counts are identical.
- **Configuration:** the only differences are:
  - the `generated` source set and its tasks are back;
  - `api` lists `files(sourceSets.generated.output)`;
  - the 45 test classpaths gain `build/classes/java/generated`, plus an empty `build/resources/generated`, because a
    source set's output includes its resources folder.
- **Checks:** from clean, `compileTestJava`, `checkstyleMain`, `checkstyleGenerated`, `checkstyleTest`,
  `:aeron-archive:loggingTest` and `:aeron-system-tests:compileTestJava` all pass.

**Performance** (fresh clones, clean trees, back to back; seconds):

| Scenario | `master` | Option A | Option B |
|---|---|---|---|
| `clean assemble -x javadoc --no-build-cache --parallel`, 6 runs, middle two | 5.43 / 5.53 | 5.82 / 5.92 | **5.41 / 5.50** |
| Edit hand-written `aeron-archive` code, `compileJava`, median of 5 | 1.23 (before 3a) | 1.50 | **1.16** |
| Edit hand-written `aeron-cluster` code, `compileJava`, median of 5 | 1.27 (before 3a) | 1.67 | **1.22** |

Option B removes both of 3a's costs: parallel clean builds are back level with `master`, and edits are back to their
pre-3a times.

**What B gives up compared with A:**

- **IDE model:** it is the same as on `master`. The codecs are a separate `generated` source set, and `main` sees
  them as a compiled-classes folder, so navigating from hand-written code may open compiled classes rather than the
  generated source. Unverified in an IDE either way.
- **More wiring:** about 25 lines per module (source set, `api` file dependency, jar, sources jar,
  `mainSourceElements` artifact) where A has 2. All of it is declarative and inferred, though, with no raw paths or
  manual task dependencies.

### Step 4: finding configuration cache problems

These task sets were run with `--configuration-cache --configuration-cache-problems=warn`:

- `assemble`, `javadoc` and `publishToMavenLocal`;
- `:aeron-client:test` and `:aeron-driver:loggingTest`;
- `generateCppCodecs`;
- the client's config-doc and validation tasks;
- `testReport`, `tarTestLogs` and `asciidoctorGithub`.

Only one problem was reported: the `doFirst` in every module's `javadoc` task called `configurations` at execution
time, which also made `javadoc` and `publishToMavenLocal` fail. Everything else stored without problems.

Tasks that can't be run locally were checked by reading the code:

- `TutorialPublishTask` calls `getProject()` in its task action, which isn't allowed (step 4b).
- `AsciidoctorPreprocessTask` (field initialisers) and `SonatypeCentralPortalUploadRepositoryTask` (constructor) only
  call `getProject()` while the task is being created, which is allowed.

### Step 4a: Javadoc offline links

Previously the `javadoc` task's `doFirst` resolved `javadocLinks`, unzipped each javadoc jar's `element-list` and
`package-list` into `build/javadoc-links/<name>`, and added `-linkoffline` options. Resolving configurations and
calling `copy`/`zipTree` at execution time isn't supported by the configuration cache.

- **`ExtractJavadocLinksTask`** (in `buildSrc`) does the extraction. It takes the resolved artifacts through
  `ArtifactCollection.getResolvedArtifacts()` and the jars as inputs, uses the injected `ArchiveOperations` and
  `FileSystemOperations`, and writes to the same `build/javadoc-links/<name>` directories.
- **`javadoc`** takes that task's output as an input, which also gives it the task dependency. It sets
  `linksOffline` when configured, from the declared `javadocLinks` dependencies, which need no resolution. The link URL
  now uses the declared version rather than the resolved one. They are the same here, because the build forces the
  Agrona version and fails on version conflicts.

Verified:

- **Artifacts:** 0 differences against a same-SHA baseline at `6894cb9be1`. The Javadoc jars' HTML contains the Agrona
  links, so they are covered.
- **Configuration cache:** `publishToMavenLocal` stores its entry with no problems. Running `javadoc` twice stores the
  entry, then reuses it with every task up to date.
- **Extracted files:** `element-list` is extracted to `build/javadoc-links/agrona` as before.

### Step 4b: `TutorialPublishTask`

The task action called `getProject()` to find its clone directory (`build/tmp/tutorialPublish`) and the repository's
`.git` directory. Both are now captured in field initialisers when the task is created, the same style
`AsciidoctorPreprocessTask` already uses, and the action only uses the fields.

The task clones and pushes to the GitHub wiki, so it can't be run for real here. It was checked with a remote name
that doesn't exist (`-Paeron.ci.remoteName=no-such-remote`), which makes it stop in `getWikiUri()` before any network
access:

- **Before:** with `--configuration-cache`, it failed with "Invocation of 'Task.project' by task
  ':aeron-samples:tutorialPublish' at execution time is unsupported with the configuration cache".
- **After:** it fails with its own "unable to find origin URI", the same as without the configuration cache.

### Step 4c: configuration cache invalidation

With the configuration cache, Gradle reuses its stored configuration unless an input it tracked has changed. Every
value below must either invalidate the entry (if read during configuration) or be re-evaluated on each run (if read
during execution). Checked in a scratch clone with a clean working tree, on `5f3089f85d`, with
`--configuration-cache`:

| Value | Result |
|---|---|
| Git SHA (`GitShaService`, evaluated during execution) | Entry reused. After an uncommitted edit, `compileJava` reran and `AeronVersion.GIT_SHA` became `5f3089f85d+guilty`. Reverting the edit restored `5f3089f85d`, with the compile served from the build cache. |
| `version.txt` | "configuration cache cannot be reused because file 'version.txt' has changed"; the new version was compiled into `AeronVersion`. |
| `-Dcodec.target.dir` | "...because system property 'codec.target.dir' has changed"; the codecs were written to the new directory. |
| `-Daeron.test.system.aeron.dir` (and the other `aeron.test.system.*` properties read the same way) | "...because system property 'aeron.test.system.aeron.dir' has changed". |
| `BUILD_JAVA_VERSION` (read by `BuildJavaVersion` in `buildSrc`) | "...because environment variable 'BUILD_JAVA_VERSION' has changed". Gradle tracks environment reads in `buildSrc` code too. |
| `AERON_*` environment variables (a `Test` input) | Entry reused, and `:aeron-test-support:test` reran when `AERON_CC_PROBE=1` was set. |

No changes were needed. The Sonatype and signing credentials are read during configuration through
`findProperty`/`System.getenv`, the same way as `BUILD_JAVA_VERSION`, so changing them invalidates the entry too.
Gradle encrypts configuration cache entries, so credentials stored in them are not kept in plain text.

## Decisions

### Test fixtures deferred: keep the source layout (October 2026)

`aeron-system-tests` depends on the whole test output of `aeron-client` and `aeron-archive`, through each module's
hand-built `tests` configuration. An analysis of the system tests' sources, including same-package references that
need no import, found:

- **`aeron-client`:** none of its 56 test classes are used, so the dependency is unnecessary.
- **`aeron-archive`:** 3 of its 39 test classes are used: `ArchiveTests`, `FailControlResponseListener` and
  `FailRecordingEventsListener`.

The standard Gradle approach is the `java-test-fixtures` plugin, with those 3 helpers moved to
`aeron-archive/src/testFixtures/java`. **Decision: keep the source layout for now.** Moving source files is a visible
change that the maintainers should weigh in on. Step 3c therefore stays build-only.

Revisit later: moving the 3 helpers to test fixtures would let the test jar go entirely. System tests would then
depend on a small, explicit set of helpers rather than on all of archive's tests.

### Step 3a: generated codecs are part of `main`

In `aeron-archive` and `aeron-cluster`, the generated codecs are now added to `main` with
`sourceSets.main.java.srcDir(tasks.named('generateCodecs'))`. Gradle infers from that that compiling, packaging the
sources and building Javadoc all depend on `generateCodecs`. Removed:

- the `generated` source set, and its `compileGeneratedJava`, `checkstyleGenerated` and related tasks;
- `api files('build/classes/java/generated')`;
- the manual `compileJava.dependsOn 'compileGeneratedJava'` and `compileGeneratedJava.dependsOn 'generateCodecs'`;
- `jar { from sourceSets.generated.output }`;
- the hand-written `sourcesJar` tasks (the standard `withSourcesJar()` now includes the generated sources);
- `javadoc.dependsOn generateCodecs`;
- all `source += …sourceSets.generated…` lines in cluster, samples and `aeron-all`;
- the `evaluationDependsOn` calls added in 2f. `sourceSets.main` exists in every project from the start, and its
  source directories are live, so the reading project no longer depends on evaluation order.

Checkstyle needs no special handling. `checkstyleGenerated` already ran in `check` and passed, so the generated code
meets the rules; it is now checked as part of `checkstyleMain`.

Verified:

- **Artifacts:** 0 differences from a clean build, including the jars, the sources jars (which must contain the
  generated codecs) and the Javadoc jars.
- **Configuration:** every difference is expected:
  - the `generated` source set's configurations and tasks are gone;
  - `compileJava` has no manual `dependsOn`;
  - the `files(...)` dependency is gone;
  - `mainSourceElements` now includes `build/generated-src`;
  - in all 45 affected test classpaths, the only change is that the two `build/classes/java/generated` directories
    have gone. Those classes are now in each module's `main` output.
- **Behaviour:** editing a schema reruns `generateCodecs`. `compileJava` stays up to date when the generated Java is
  unchanged, which is correct.
- **Builds and tests:** `compileTestJava` in all modules, `checkstyleMain`, `checkstyleTest` and
  `:aeron-archive:loggingTest` pass. Nothing in CI, `cppbuild` or the scripts refers to the removed tasks.

#### 3a follow-up: what changes for developers

How the generated codecs get into the jar, before and after:

| | Before 3a | After 3a |
|---|---|---|
| Generated `.java` files | `build/generated-src` | `build/generated-src` (unchanged) |
| Compiled by | a separate task, `compileGeneratedJava` | `compileJava`, together with the hand-written code |
| `.class` files | `build/classes/java/generated` | `build/classes/java/main` |
| How `main` sees them | a compiled-classes folder on its classpath (`api files(...)`) | as sources in its own source set |
| Into the jar | an extra `from sourceSets.generated.output` | the normal jar contents |

**IDE model**, as Gradle reports it for archive (cluster is the same):

- **Before:** two source sets. `generated` has `build/generated-src`. `main` has only `src/main/java`, with
  `build/classes/java/generated` on its compile classpath.
- **After:** one source set. `main` has `src/main/java` and `build/generated-src`.

The expected effect in IntelliJ is that the codecs become a generated source root of the main module, so navigating
to them from hand-written code opens the generated source rather than compiled classes. This is **not verified in an
IDE**; someone who works on Aeron in IntelliJ should check it after a Gradle sync. As before, `generateCodecs` (or any
build) must run once, because an IDE sync doesn't run it.

**Incremental compile cost.** Measured in fresh clones, with a unique edit per run and `--no-build-cache`: median of 5
runs for edits, 3 for clean builds, in seconds.

| Scenario | Before 3a | After 3a |
|---|---|---|
| Edit hand-written code in `aeron-archive`, then `compileJava` | 1.23 | 1.55 |
| Edit hand-written code in `aeron-cluster`, then `compileJava` | 1.27 | 1.60 |
| `aeron-archive` `compileJava` after a clean | 1.36 | 1.40 |
| `aeron-cluster` `compileJava` after a clean | 1.46 | 1.50 |

So each edit in archive or cluster costs about 0.3s more, while the total work after a clean is unchanged. The cause
predates this work: `compileJava` always does a full recompile, because none of the `aeron-annotations` processors
(`VersionProcessor`, `ConfigProcessor`, `CounterProcessor`, `CborEventLoggerProcessor`) are registered as incremental.
Gradle's log says: "Full recompilation is required because io.aeron.eventlog.CborEventLoggerProcessor is not
incremental". Before 3a the codecs were in a separate compile task and escaped those full recompiles; now they're
included.

Other visible changes:

- the `compileGeneratedJava` and `checkstyleGenerated` tasks no longer exist;
- `build/classes/java/generated` is no longer produced.

Nothing in CI, `cppbuild` or the scripts uses them, but individual developers' scripts or run configurations might.

### Generated codecs compiled separately, wired through outputs (October 2026)

**Decision: option B.** Step 3a originally moved the generated codecs into the `main` source set (option A). That
cost about 0.3s per edit in archive and cluster, and about 0.4s on parallel clean builds, because codec compilation
moved onto the client → driver → archive → cluster critical path.

Option B (see the prototype above) keeps 3a's removal of raw paths, manual `dependsOn` calls and hand-written
`sourcesJar` tasks, but compiles the codecs in their own `generated` source set again. With B:

- performance is back to `master` levels for parallel clean builds, and to pre-3a levels for edits;
- the published artifacts are identical to A's.

The trade-off is that B doesn't get A's expected (unverified) IDE navigation improvement; the IDE model is the same
as on `master`.

Option C, making the `aeron-annotations` processors incremental, is still worth proposing (question 2). It would
speed up edits in every module that uses them.

## Questions for the Aeron engineers

1. **Generated codecs (step 3a and option B).** The codecs are compiled in their own `generated` source set, so the
   IDE model is the same as on `master`. Option A, compiling them as part of `main`, was expected to give better
   IDE navigation to the codecs, but cost build time and was rejected. Would IDE users on the team value that
   navigation enough to revisit it? If so, could someone check whether it actually works in IntelliJ, using a build
   of commit `7c58114e8a`?
2. **Incremental annotation processors.** Would the team accept making the `aeron-annotations` processors
   incremental? That means registering them in `META-INF/gradle/incremental.annotation.processors` as isolating or
   aggregating, which may need changes to how they report originating elements.
   - Today every edit to client, driver, archive or cluster recompiles the whole module, on `master` too.
   - Making them incremental would speed up edits in all those modules.
   - `ConfigProcessor` and `CounterProcessor` write one aggregated `.dat` file each, so they would be "aggregating".
     `VersionProcessor` generates a class per annotated type, so it may be "isolating".
     `CborEventLoggerProcessor` needs looking at.
3. **Test fixtures (decision above).** Is moving the 3 archive test helpers used by the system tests into
   `src/testFixtures/java` acceptable?
4. **Config/counter validation (step 2e).** `validateConfigExpectations` and `validateCounterExpectations` report
   missing C `#define`s but exit 0, and CI doesn't run them. Should they fail the build and run in CI?
5. **`--add-opens`.** The flag is a runtime requirement because Agrona's `UnsafeApi` uses
   `jdk.internal.misc.Unsafe`. Is removing that dependency on the Agrona roadmap?
6. **Parallel execution (step 1).** Can test tasks from different modules safely run at the same time? They start
   media drivers and share Aeron directories and ports. If so, `org.gradle.parallel=true` cut a clean build from about
   8.1s to about 5.2s.
7. **Contents of the archive test jar (step 3b).** `aeron-archive`'s test jar is consumed by `aeron-system-tests`
   through the `tests` configuration. It now also contains the 117 versioned test codec classes
   (`io.aeron.archive.codecs.v6`, `io.aeron.archive.codecs.mark.v0` and `.v1`), which were previously compiled
   separately and left out.
   - Nothing in the repo clashes: those packages exist nowhere else, and the system tests compile.
   - Is anything outside this repo consuming the test jars, for example premium or downstream builds, that could be
     affected by the extra classes?
   - Since step 3c the jar is also named differently: `aeron-archive-<version>-tests.jar`. It used to be
     `aeron-archive-<version>-test-extension 'base' property 'archivesName'.jar`, because of a bug. Only archive builds
     a test jar now. Anything that looked for the old file names would need updating.
   - The same applies to cluster's test output (`io.aeron.cluster.codecs.v5`, `.v12` and `.mark.v0`), though nothing
     in the repo consumes cluster's test jar.
