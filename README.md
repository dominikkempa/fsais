fSAIS - External memory suffix array construction using inducing
================================================================


Description
-----------

fSAIS is an implementation of the external-memory suffix array
construction algorithm. The algorithm was described in the paper

    @inproceedings{kkpz17alenex,
      author =    {Juha K{\"{a}}rkk{\"{a}}inen and Dominik Kempa
                   and Simon J. Puglisi and Bella Zhukova},
      title =     {Engineering External Memory Induced Suffix Sorting},
      booktitle = {19th Meeting on Algorithm Engineering and Experimentation
                   (ALENEX 2017)},
      pages     = {98--108},
      year      = {2017},
      doi       = {10.1137/1.9781611974768.8},
    }

The latest version of fSAIS is available from
https://github.com/dominikkempa/fsais.



Requirements
------------

fSAIS has no external dependencies (libraries, cmake, etc).
It only requires:
- g++ compiler supporting the -std=c++0x flag (all modern versions)
- A 64-bit operating system. The current version has been tested
  on Linux/PC.



Compilation and usage
---------------------

The package contains a single Makefile in the main directory. Type
`make` to build the executable. For usage instructions, run the
program without any arguments.

### Example

The simplest usage of fSAIS is as follows. Suppose the text is located
in `/data/input.txt`. Then, to compute the suffix array of `input.txt`
type:

    $ ./construct_sa /data/input.txt


This will write the output suffix array to `/data/input.txt.sa5`. Each
element of the suffix array is encoded using 40-bit integers, i.e.,
the output suffix array will take up 5n bytes of disk space. By
default, the algorithm uses 3.5GiB of RAM for computation and it
assumes that the input text is over byte alphabet (see below for
explanation on how to adjust this). A more advanced usage is
demonstrated below.

    $ ./construct_sa ./input.txt -m 8gi -o ../input.txt.sa


Explanation:
- The -m flag allows specifying the amount of RAM used during the
  computation (in bytes). In this example, the RAM limit is set to 8gi
  = 8 * 2^30 bytes (see below).
- The -o flag allows specifying the location and filename of the
  output suffix array. The default location and filename is the same
  as input text, with the appended ".saX" suffix, where X is the used
  integer size (by default: 5 byte).

Notes:
- The argument of the -m flag (RAM used during the computation) can be
  specified either explicitly or using common suffixes such as K, M,
  G, T, Ki, Mi, Gi, Ti, which respectively correspond to multipliers:
  10^3, 10^6, 10^9, 10^12, 2^10, 2^20, 2^30, 2^40. Suffix names are
  not case-sensitive, e.g., Ti = ti, k = K.
- The flags specifying RAM usage, output filename, etc. can be given
  in any order.
- Filenames passed as command-line arguments can be given as relative
  paths, e.g., `../input.txt` and `~/data/input.txt` are valid paths,
  see also example above.
- To enable additional statistics about the computation (alternative
  counter of I/O volume and tracing of the disk usage), uncomment line
  with AUX_DISK_FLAGS in the Makefile. When this flag is enabled, the
  computation could slow down thus this flag is disabled by default.
- To change the type used to encode characters of the input text or
  the integer type used to encode positions in the text, adjust types
  "char_type" and "text_offset_type" as well as the value of the
  variable "text_alphabet_size" in the source file ./src/main.cpp.



Troubleshooting
---------------

1. I am getting an error about the exceeded number of opened files.

Solution: The error is caused by the operating system imposing a limit
on the maximum number of files opened by a program. The limit can be
increased with the `ulimit -n newlimit` command. However, in Linux the
limit cannot be increased beyond the so-called "hard limit", which is
usually only few times larger. Furthermore, this is a temporary
solution that needs to repeated every time a new session is
started. To increase the limits permanently, edit (as a root) the file
`/etc/security/limits.conf` and add the following lines at the end
(including the asterisks):


    * soft nofile 128000
    * hard nofile 128000


This increases the limit to 128000 (use larger values if necessary).
The new limits apply (check with `ulimit -n`) after starting new
session.



Limitations
-----------

- At present the only limitation in the usage of the algorithm is the
  need to ensure that the limit for the number of opened files in the
  system is sufficiently large to prevent the above error. This
  technical shortcoming will be eliminated in the future versions of
  fSAIS.



Terms of use
------------

fSAIS is released under the MIT/X11 license. See the file LICENCE for
more details. If you use this code, please cite the paper mentioned
above.



Authors
-------

fSAIS was implemented by:
- [Dominik Kempa](https://scholar.google.com/citations?user=r0Kn9IUAAAAJ)
- [Juha Karkkainen](https://scholar.google.com/citations?user=oZepo1cAAAAJ)
