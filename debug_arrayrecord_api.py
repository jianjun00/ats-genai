#!/usr/bin/env python3

import sys
import os

print("=== ArrayRecord API Investigation ===")

try:
    import array_record
    print(f"✅ array_record imported successfully")
    print(f"Package location: {array_record.__file__}")
    print(f"Package __path__: {getattr(array_record, '__path__', 'None')}")
    
    # Check all attributes
    attrs = [attr for attr in dir(array_record) if not attr.startswith('_')]
    print(f"Public attributes: {attrs}")
    
    # Try to find ArrayRecordWriter or similar classes
    writer_attrs = [attr for attr in dir(array_record) if 'writer' in attr.lower()]
    print(f"Writer-related attributes: {writer_attrs}")
    
    record_attrs = [attr for attr in dir(array_record) if 'record' in attr.lower()]
    print(f"Record-related attributes: {record_attrs}")
    
    # Check if it has python submodule
    try:
        from array_record import python as ar_python
        print(f"✅ array_record.python imported")
        python_attrs = [attr for attr in dir(ar_python) if not attr.startswith('_')]
        print(f"Python module attributes: {python_attrs}")
        
        # Check for writer classes in python module  
        writer_attrs_python = [attr for attr in dir(ar_python) if 'writer' in attr.lower() or 'Writer' in attr]
        print(f"Writer classes in python module: {writer_attrs_python}")
        
    except ImportError as e:
        print(f"❌ array_record.python import failed: {e}")
    
    # Try common naming patterns
    possible_names = [
        'ArrayRecordWriter',
        'Writer', 
        'RecordWriter',
        'ArrayWriter'
    ]
    
    for name in possible_names:
        if hasattr(array_record, name):
            cls = getattr(array_record, name)
            print(f"✅ Found {name}: {cls}")
            print(f"   Methods: {[m for m in dir(cls) if not m.startswith('_')]}")
        else:
            print(f"❌ No {name} found")
    
    # Check if there's a different module structure
    try:
        # Maybe it's in a submodule
        from array_record.python import array_record_writer
        print(f"✅ Found array_record_writer module")
        writer_attrs = [attr for attr in dir(array_record_writer) if not attr.startswith('_')]
        print(f"array_record_writer attributes: {writer_attrs}")
    except ImportError as e:
        print(f"❌ array_record.python.array_record_writer import failed: {e}")
    
    try:
        from array_record.python.array_record_writer import ArrayRecordWriter
        print(f"✅ Found ArrayRecordWriter class!")
        print(f"   Class: {ArrayRecordWriter}")
        print(f"   Methods: {[m for m in dir(ArrayRecordWriter) if not m.startswith('_')]}")
        
        # Try to get help/docstring
        print(f"   Docstring: {ArrayRecordWriter.__doc__}")
        
    except ImportError as e:
        print(f"❌ ArrayRecordWriter import failed: {e}")

except ImportError as e:
    print(f"❌ array_record package not available: {e}")

print("\n=== TensorFlow ArrayRecord Check ===")
try:
    import tensorflow as tf
    print(f"TensorFlow version: {tf.__version__}")
    
    # Check if TF has ArrayRecord support
    if hasattr(tf.io, 'ArrayRecord'):
        print(f"✅ Found tf.io.ArrayRecord")
    if hasattr(tf.io, 'ArrayRecordWriter'):
        print(f"✅ Found tf.io.ArrayRecordWriter") 
    if hasattr(tf.io, 'ArrayRecordReader'):
        print(f"✅ Found tf.io.ArrayRecordReader")
        
    # Check all tf.io attributes for array/record
    tf_io_attrs = [attr for attr in dir(tf.io) if 'array' in attr.lower() or 'record' in attr.lower()]
    print(f"TF IO array/record attributes: {tf_io_attrs}")
    
except ImportError as e:
    print(f"❌ TensorFlow not available: {e}")

print("\n=== Alternative Package Check ===")
# Maybe it's using a different package name
alternative_packages = [
    'riegeli',
    'grain', 
    'tf_grain',
    'seqio'
]

for pkg in alternative_packages:
    try:
        module = __import__(pkg)
        print(f"✅ Found {pkg} package")
        if hasattr(module, 'ArrayRecordWriter'):
            print(f"  ✅ {pkg} has ArrayRecordWriter")
    except ImportError:
        print(f"❌ {pkg} not available")