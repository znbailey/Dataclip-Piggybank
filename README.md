# Dataclip Piggybank #
These are some general-purpose Pig UDFs in use at Dataclip (http://dataclip.com) every day.

# Building #
Build using ant. This will produce a JAR in the build directory that can be registered in your pig script.

# Dependencies #
These UDFS were written against Pig 0.7.0 and are intended to compile against that version of the API. The
Pig 0.7.0+9 JAR is included for compiling against.

# Licenses #
Dataclip code is licensed under Apache 2. See the accompanying LICENSE file and the notices in the source files
for more information.

This also contains a repackaged version of the Aho-Corasick string matching algorithm as originally coded by Danny Yoo.
The code was updated by Zach Bailey at Dataclip to support Java 1.5 Generics.

For license information see the LICENSE file included in the same directory as the Aho-Corasick source files.
Source: http://hkn.eecs.berkeley.edu/~dyoo/java/index.html