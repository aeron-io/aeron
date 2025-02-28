/*
 * Copyright 2014-2024 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <thread>
#include <chrono>

using namespace std;

int main(int argc, char **argv)
{
    cout << "system_clock resolution              ";
    cout << chrono::system_clock::period::num << "/";
    cout << chrono::system_clock::period::den << "\n";

    cout << "steady_clock resolution              ";
    cout << chrono::steady_clock::period::num << "/";
    cout << chrono::steady_clock::period::den << "\n";

    cout << "high_resolution_clock resolution     ";
    cout << chrono::high_resolution_clock::period::num << "/";
    cout << chrono::high_resolution_clock::period::den << "\n";

    auto start = chrono::steady_clock::now();
    this_thread::sleep_for(chrono::nanoseconds(1));
    auto end = chrono::steady_clock::now();

    auto diff = end - start;

    cout << "sleep_for min duration:              ";
    cout << std::chrono::duration<long, std::nano>(diff).count() << " ns\n";

    start = chrono::steady_clock::now();
    this_thread::yield();
    end = chrono::steady_clock::now();
    diff = end - start;

    cout << "sample yield duration:               ";
    cout << std::chrono::duration<long, std::nano>(diff).count() << " ns\n";

    start = chrono::steady_clock::now();
    static_cast<void>(chrono::high_resolution_clock::now());
    end = chrono::steady_clock::now();
    diff = end - start;

    cout << "high_resolution_clock::now duration: ";
    cout << std::chrono::duration<long, std::nano>(diff).count() << " ns\n";

    start = chrono::steady_clock::now();
    static_cast<void>(chrono::steady_clock::now());
    end = chrono::steady_clock::now();
    diff = end - start;

    cout << "steady_clock::now duration:          ";
    cout << std::chrono::duration<long, std::nano>(diff).count() << " ns\n";

    start = chrono::steady_clock::now();
    static_cast<void>(chrono::system_clock::now());
    end = chrono::steady_clock::now();
    diff = end - start;

    cout << "system_clock::now duration:          ";
    cout << std::chrono::duration<long, std::nano>(diff).count() << " ns\n";

    start = chrono::steady_clock::now();
    end = chrono::steady_clock::now();
    diff = end - start;

    cout << "no op duration:                      ";
    cout << std::chrono::duration<long, std::nano>(diff).count() << " ns\n";

    start = chrono::steady_clock::now();

    chrono::steady_clock::time_point now = chrono::steady_clock::now();
    long count = chrono::duration<long, std::nano>(now.time_since_epoch()).count();

    end = chrono::steady_clock::now();
    diff = end - start;

    cout << "nano_clock duration:                 ";
    cout << std::chrono::duration<long, std::nano>(diff).count() << " ns\n";

    cout << "nano_clock sample:                   " << count << "\n";

    cout << "duration ratio:                      " << chrono::duration<double, std::ratio<1, 1>>(diff).count() << "\n";

    return 0;
}
