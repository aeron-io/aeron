#
# Copyright 2014-2025 Real Logic Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

function Add-Arg
{

    [CmdletBinding()]
    param (
        [string]$aggregate,
        [string]$toAppend
    )

    if ($aggregate) { "$aggregate $toAppend" } else { $toAppend }
}

$CmakeExtraArgs = ""
$CmakeBuildParallelLevel = [Environment]::ProcessorCount
$DeleteBuildDir = $true
$BuildConfig = "Release"

for ($i = 0; $i -lt $Args.count; $i++)
{
    $arg = $Args[$i]
    if ($arg -eq "--help")
    {
        Write-Host "[--c-warnings-as-errors] [--cxx-warnings-as-errors] [--build-aeron-driver] [--link-samples-client-shared] [--build-archive-api] [--skip-rmdir] [--slow-system-tests] [--no-system-tests] [--debug-build] [--sanitise-build]  [--gradle-wrapper path_to_gradle] [--help]"
    }
    elseif ($args -eq "--cmake-extra-args")
    {
        if ($i + 1 -eq $Args.count)
        {
            throw "--cmake-extra-args requires a parameter"
        }

        $nextArg = $Args[$i + 1]
        $CmakeExtraArgs = if ($CmakeExtraArgs) { "$CmakeExtraArgs $nextArg" } else { $nextArg }
        $i++
    }
    elseif ($arg -eq "--c-warnings-as-errors")
    {
        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DC_WARNINGS_AS_ERRORS=ON"
    }
    elseif ($arg -eq "--cxx-warnings-as-errors")
    {
        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DCXX_WARNINGS_AS_ERRORS=ON"
    }
    elseif ($arg -eq "--cxx-hide-deprecation-message")
    {
        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DAERON_HIDE_DEPRECATION_MESSAGE=ON"
    }
    elseif ($arg -eq "--link-samples-client-shared")
    {
        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DLINK_SAMPLES_CLIENT_SHARED=ON"
    }
    elseif ($arg -eq "--skip-rmdir")
    {
        $DeleteBuildDir = $false
    }
    elseif ($arg -eq "--no-tests")
    {
        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DAERON_TESTS=OFF"
    }
    elseif ($arg -eq "--no-system-tests")
    {
        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DAERON_SYSTEM_TESTS=OFF"
    }
    elseif ($arg -eq "--no-unit-tests")
    {
        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DAERON_UNIT_TESTS=OFF"
    }
    elseif ($arg -eq "--slow-system-tests")
    {
        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DAERON_SLOW_SYSTEM_TESTS=ON"
    }
    elseif ($arg -eq "--no-unit-tests")
    {
        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DAERON_UNIT_TESTS=OFF"
    }
    elseif ($arg -eq "--debug-build")
    {
        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DCMAKE_BUILD_TYPE=Debug"
        $BuildConfig = "Debug"
        Write-Host "Enabling debug build"
    }
    elseif ($arg -eq "--relwithdebinfo-build")
    {
        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DCMAKE_BUILD_TYPE=RelWithDebInfo"
        $BuildConfig = "RelWithDebInfo"
        Write-Host "Enabling release with debug info build"
    }
    elseif ($arg -eq "--compiler-optimization-level" -or $arg -eq "--compiler-optimisation-level")
    {
        if ($i + 1 -eq $Args.count)
        {
            throw "--compiler-optimization-level requires a parameter"
        }
        $nextArg = $Args[$i + 1]

        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DAERON_COMPILER_OPTIMIZATION_LEVEL=$nextArg"
        Write-Host "Setting compiler optimisation level to: /O$nextArg"
        $i++
    }
    elseif ($arg -eq "--sanitise-build")
    {
        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DSANITISE_BUILD=ON"
    }
    elseif ($arg -eq "--gradle-wrapper")
    {
        if ($i + 1 -eq $Args.count)
        {
            throw "--gradle-wrapper requires a parameter"
        }
        $nextArg = $Args[$i + 1]

        $CmakeExtraArgs = Add-Arg $CmakeExtraArgs "-DGRADLE_WRAPPER=$nextArg"
        Write-Host "Setting -DGRADLE_WRAPPER=$nextArg"
        $i++
    }
    elseif ($arg -eq "--no-parallel")
    {
        $CmakeBuildParallelLevel = 1
        Write-Host "Disabling parallel build"
    }
    elseif ($arg -eq "--parallel-cpus")
    {
        if ($i + 1 -eq $Args.count)
        {
            throw "--gradle-wrapper requires a parameter"
        }
        $nextArg = $Args[$i + 1]

        $CmakeBuildParallelLevel = $nextArg
        Write-Host "Using $CmakeBuildParallelLevel cpus"
        $i++
    }
    else
    {
        Write-Error "Unknown option $arg"
        throw "Use --help for help"
    }
}

$BuildDir = "$PSScriptRoot\$BuildConfig"
$SourceDir = "$PSScriptRoot\.."
$CMakeVersion = "4.2.1"
$CMakeDirName = "cmake-$CMakeVersion-windows-x86_64"
$CMakeArchive = "$CMakeDirName.zip"
$CMakePath = "$PSScriptRoot\$CMakeDirName"
$OldPath = $env:Path

try
{
    if (-not (Test-Path $CMakePath))
    {
        Write-Host "Installing $CMakeArchive ..."

        $client = New-Object System.Net.WebClient
        $client.DownloadFile("https://github.com/Kitware/CMake/releases/download/v$CMakeVersion/$CMakeArchive", "$PSScriptRoot\$CMakeArchive")

        Push-Location $PSScriptRoot
        Expand-Archive -LiteralPath "$CMakeArchive" -DestinationPath "$PSScriptRoot"
        Remove-Item "$CMakeArchive"
        Pop-Location

        Write-Host "Success: $CMakePath"
    }

    if ((Test-Path $BuildDir) -and ($DeleteBuildDir))
    {
        Remove-Item -Path $BuildDir -Force -Recurse
    }

    if (-not (Test-Path $BuildDir))
    {
        [void](New-Item -Path $BuildDir -Type Directory -Force)
    }

    Push-Location -Path $BuildDir

    $vsPath = &(Join-Path ${env:ProgramFiles(x86)} "\Microsoft Visual Studio\Installer\vswhere.exe") -property installationpath
    Write-Host $vsPath
    Import-Module (Join-Path $vsPath "Common7\Tools\Microsoft.VisualStudio.DevShell.dll")
    Enter-VsDevShell -VsInstallPath $vsPath -SkipAutomaticLocation

    $env:Path = "$CMakePath\bin;$env:Path"

    cmake $CmakeExtraArgs $SourceDir
    cmake --build . --config $BuildConfig --parallel $CmakeBuildParallelLevel
    if (-not $?)
    {
        Write-Host "Compile Failed"
        Exit 1
    }

    ctest -C $BuildConfig --output-on-failure --timeout 2000
}
finally
{
    Pop-Location
    $env:Path = $OldPath
}


