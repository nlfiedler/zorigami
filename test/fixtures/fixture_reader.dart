//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:io';

String fixture(String name) => File('test/fixtures/$name').readAsStringSync();
