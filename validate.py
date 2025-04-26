#!/usr/bin/env python3
"""
Pre-deployment validation script for NL2SPARQL

This script performs various checks to validate the codebase before deployment:
1. Syntax checking (compiles all Python files to catch syntax errors)
2. Import checking (tests for circular imports and missing modules)
3. Config validation
4. Basic runtime tests
"""

import importlib
import os
import py_compile
import sys
import traceback
from pathlib import Path

# Root directory for the project
ROOT_DIR = Path(__file__).parent.absolute()


def check_syntax(directory='.'):
    """Check syntax of all Python files in the directory and its subdirectories."""
    print("Running syntax checks...")
    errors = []
    
    for root, dirs, files in os.walk(directory):
        # Skip __pycache__ directories
        if '__pycache__' in dirs:
            dirs.remove('__pycache__')
        
        # Skip hidden directories
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                try:
                    py_compile.compile(file_path, doraise=True)
                except Exception as e:
                    errors.append(f"Syntax error in {file_path}: {str(e)}")
    
    return errors

def check_circular_imports(modules_to_check):
    """Check for circular imports in modules."""
    print("Checking for circular imports...")
    errors = []
    
    for module_path in modules_to_check:
        try:
            # Use importlib to check imports
            importlib.import_module(module_path)
        except ImportError as e:
            errors.append(f"Import error in {module_path}: {str(e)}")
    
    return errors

def validate_configs():
    """Validate configuration files and settings."""
    print("Validating configuration files...")
    errors = []
    
    # List of config modules to validate
    config_modules = [
        'config.api_config',
        'config.logging_config',
        'config.agent_config'
    ]
    
    for module_name in config_modules:
        try:
            module = importlib.import_module(module_name)
        except Exception as e:
            errors.append(f"Error in {module_name}: {str(e)}")
            continue
    
    return errors

def run_basic_tests():
    """Run basic runtime tests."""
    print("Running basic runtime tests...")
    errors = []
    
    # Test imports for critical components
    critical_components = [
        # Core modules
        'api',
        'master.global_master',
        # Updated to use the correct path that exists
        'master.base', 
        'slaves.slave_pool_manager',
        'slaves.slave_pool',
        # Utils
        'utils.health_checker',
        'utils.monitoring',
        'utils.load_balancer',
    ]
    
    for component in critical_components:
        try:
            importlib.import_module(component)
        except Exception as e:
            errors.append(f"Failed to import {component}: {str(e)}")
            traceback.print_exc()
    
    return errors

def main():
    """Main validation function."""
    print("=== NL2SPARQL Pre-deployment Validation ===")
    
    # Change directory to project root
    os.chdir(ROOT_DIR)
    
    # Add current directory to path to allow imports
    sys.path.insert(0, str(ROOT_DIR))
    
    all_errors = []
    
    # Run syntax checks
    errors = check_syntax()
    if errors:
        all_errors.extend(errors)
        print(f"✗ Syntax check failed with {len(errors)} errors:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("✓ Syntax check passed")
    
    # Core modules to check for circular imports
    core_modules = [
        'api', 
        'main',
        'master.global_master',
        'master.domain_master',
        'slaves.slave_pool_manager',
        'slaves.slave_pool',
        'utils.health_checker',
        'utils.monitoring'
    ]
    
    # Check for circular imports
    errors = check_circular_imports(core_modules)
    if errors:
        all_errors.extend(errors)
        print(f"✗ Import check failed with {len(errors)} errors:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("✓ Import check passed")
    
    # Validate configs
    errors = validate_configs()
    if errors:
        all_errors.extend(errors)
        print(f"✗ Config validation failed with {len(errors)} errors:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("✓ Config validation passed")
    
    # Run basic tests
    errors = run_basic_tests()
    if errors:
        all_errors.extend(errors)
        print(f"✗ Basic tests failed with {len(errors)} errors:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("✓ Basic tests passed")
    
    # Summary
    if all_errors:
        print(f"\n✗ Validation failed with {len(all_errors)} total errors.")
        sys.exit(1)
    else:
        print("\n✓ All validation checks passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
