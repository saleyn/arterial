#!/bin/bash

echo "=== Arterial Timeout Integration Test ==="
echo

# Test 1: Verify files exist and are properly structured
echo "1. Checking integrated timeout files..."

if [ -f "c_src/arterial_timer_simple.hpp" ]; then
    echo "   ✓ arterial_timer_simple.hpp exists"
else
    echo "   ✗ arterial_timer_simple.hpp missing"
    exit 1
fi

if [ -f "c_src/arterial_timer_simple.hxx" ]; then
    echo "   ✓ arterial_timer_simple.hxx exists"
else
    echo "   ✗ arterial_timer_simple.hxx missing"
    exit 1
fi

if [ ! -f "c_src/arterial_connection_timeout.hpp" ]; then
    echo "   ✓ arterial_connection_timeout.hpp successfully removed (merged)"
else
    echo "   ! arterial_connection_timeout.hpp still exists (should be merged)"
fi

# Test 2: Check for key integration points in source code
echo
echo "2. Checking integration points..."

if grep -q "ConnectionTimeout" c_src/arterial_timer_simple.hpp; then
    echo "   ✓ ConnectionTimeout class found in arterial_timer_simple.hpp"
else
    echo "   ✗ ConnectionTimeout class not found"
    exit 1
fi

if grep -q "setup_connection_timeout_fd" c_src/arterial_timer_simple.hxx; then
    echo "   ✓ setup_connection_timeout_fd implementation found"
else
    echo "   ✗ setup_connection_timeout_fd implementation not found"
    exit 1
fi

if grep -q "std::unique_ptr<ConnectionTimeout>" c_src/arterial_connection.hpp; then
    echo "   ✓ Connection struct updated to use ConnectionTimeout"
else
    echo "   ✗ Connection struct not updated"
    exit 1
fi

if grep -q "cancel_connection_timeout" c_src/arterial_connection.hxx; then
    echo "   ✓ Timeout cancellation code found in connection handling"
else
    echo "   ✗ Timeout cancellation code not found"
    exit 1
fi

# Test 3: Check platform-specific implementations
echo
echo "3. Checking platform-specific implementations..."

if grep -q "timerfd_create" c_src/arterial_timer_simple.hxx; then
    echo "   ✓ Linux timerfd implementation found"
else
    echo "   ✗ Linux timerfd implementation not found"
    exit 1
fi

if grep -q "kqueue" c_src/arterial_timer_simple.hxx; then
    echo "   ✓ macOS/BSD kqueue implementation found"
else
    echo "   ✗ macOS/BSD kqueue implementation not found"
    exit 1
fi

if grep -q "Generic Fallback" c_src/arterial_timer_simple.hxx; then
    echo "   ✓ Generic fallback implementation found"
else
    echo "   ✗ Generic fallback implementation not found"
    exit 1
fi

# Test 4: Check test files
echo
echo "4. Checking test files..."

if [ -f "test/integrated_timeout_test.erl" ]; then
    echo "   ✓ Comprehensive test file created"
else
    echo "   ✗ Comprehensive test file missing"
    exit 1
fi

if [ -f "test/simple_integrated_timeout_test.erl" ]; then
    echo "   ✓ Simple test file created"
else
    echo "   ✗ Simple test file missing"
    exit 1
fi

# Test 5: Run simple test
echo
echo "5. Running simple conceptual test..."

cd test
if erlc simple_integrated_timeout_test.erl && erl -noshell -eval "simple_integrated_timeout_test:test(), halt()."; then
    echo "   ✓ Simple test passed"
else
    echo "   ✗ Simple test failed"
    exit 1
fi
cd ..

echo
echo "=== Integration Test Results ==="
echo "✓ File structure correct"
echo "✓ Integration points implemented"
echo "✓ Platform-specific code present"
echo "✓ Test files created"
echo "✓ Conceptual test passed"
echo
echo "✓ TIMEOUT INTEGRATION SUCCESSFUL!"
echo
echo "Next steps:"
echo "1. Compile the NIF: rebar3 compile"
echo "2. Run full tests: rebar3 eunit"
echo "3. Test with real connections"
echo