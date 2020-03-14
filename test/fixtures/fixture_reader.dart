//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:io';

// Seems the tests are run from within the test directory.
String fixture(String name) => File('fixtures/$name').readAsStringSync();
