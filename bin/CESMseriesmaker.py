#!/usr/bin/env python
"""
This script provides the command-line interface (CLI) to the PyReshaper

Copyright 2015, Deepak Chandan and UCAR
"""

try:
    import Nio
    nio_available = True
except ImportError:
    nio_available = False

try:
    import netCDF4
    netcdf_available = True
except ImportError:
    netcdf_available = False

import os
import glob
import sys
import argparse
import os.path as ospath

from pyreshaper.specification import create_specifier
from pyreshaper.reshaper import create_reshaper

from asaptools.simplecomm import create_comm, SimpleComm


#==============================================================================
# specification
#==============================================================================
def specification():

    class ExtractSpecificationData(argparse.Action):
        """
        Action to extract specification data from the command line
        argument if the user has chosen to specify input files using
        the specification method.
        """
        def __call__(self, parser, namespace, values, option_string=None):
            message = ''
            if len(values) != 5:
                message = 'argument "{}" requires 5 arguments'.format(self.dest)

            if values[0].isdigit():
                err_msg = 'first argument to "{}" requires a string'.format(self.dest)
                raise ValueError(err_msg)

            if values[1].isdigit():
                err_msg = 'first argument to "{}" requires a string'.format(self.dest)
                raise ValueError(err_msg)

            model_choices = ['atm', 'lnd', 'ocn', 'ice']
            if values[2] not in model_choices:
                err_msg = 'third argument to "{0}" must be one of {1}'.format(self.dest, model_choices)
                raise ValueError(err_msg)

            try:
                values[3] = int(values[3])
            except ValueError:
                message = 'fourth argument to "{}" requires an integer'.format(self.dest)

            try:
                values[4] = int(values[4])
            except ValueError:
                message = 'fifth argument to "{}" requires an integer'.format(self.dest)

            if (values[3] > values[4]):
                message = 'fifth argument to "{}" must be >= fourth argument'.format(self.dest)

            if message:
                raise argparse.ArgumentError(self, message)

            setattr(namespace, "case", values[0])
            setattr(namespace, "root", values[1])
            setattr(namespace, "model", values[2])
            setattr(namespace, "start", values[3])
            setattr(namespace, "end", values[4])

    return ExtractSpecificationData


#==============================================================================
# Command-line Interface
#==============================================================================
def cli():

    # Choices for the netCDF backend library
    backend_choices = []
    if netcdf_available: backend_choices.append("netcdf")
    if nio_available: backend_choices.append("nio")
    if not (nio_available or netcdf_available):
        print "ERROR: Neither the PyNIO nor the netCDF4",
        print "backend library is installed."
        sys.exit(-1)

    # Top-level argument parser
    parser = argparse.ArgumentParser(prog="{0}: Convert CESM slice files to "
                                     "series files".format(__file__))

    # Mutually exclusive options
    mexcl_args = parser.add_mutually_exclusive_group(required=True)
    mexcl_args.add_argument('--files', type=str, nargs='+',
                            help='Specify a list of files')
    mexcl_args.add_argument('--spec', dest='spec', nargs='+',
                            action=specification(),
                            help='Select files using specification')

    # Output arguments
    output_args = parser.add_argument_group('Output settings',
                                            'Options for the output '
                                            'timeseries files')
    output_args.add_argument('--append', default=False, action='store_true',
                               help='Append to existing output files '
                                    '[Default: False]')
    output_args.add_argument('-d', '--deflate', default=3, type=int,
                             action='store',
                             help='Compression level for the output files. '
                                  '[Default: 3]')
    output_args.add_argument('--skip-existing', default=False,
                             action='store_true', dest='skip_existing',
                             help='Whether to skip time-series generation '
                                  'for variables with existing output files. '
                                  '[Default: False]')
    output_args.add_argument('--overwrite', default=False, action='store_true',
                             help='Whether to overwrite existing output '
                                  'files. [Default: False]')
    output_args.add_argument('--once', default=False, action='store_true',
                             help='Whether to write a "once" file with all '
                                  'metadata. [Default: False]')
    output_args.add_argument('-m', '--metadata', type=str,
                             nargs="+", default=[],
                             help='Names of a variable to be included in all '
                                  'output files. [Default: none]')
    output_args.add_argument('--odir', type=str, action='store', required=True,
                             help='Directory into which to write timeseries '
                                  'files')

    # Program arguments
    prog_args = parser.add_argument_group('Program Options')
    prog_args.add_argument('--backend', type=str, choices=backend_choices,
                           default=backend_choices[0],
                           help='netcdf backend to use')
    prog_args.add_argument('--serial', default=False, action='store_true',
                           help='Whether to run in serial (True) or parallel '
                                '(False). [Default: False]')
    prog_args.add_argument('--timecode', default=False, action='store_true',
                           help='Whether to time the code internally. '
                                '[Default: False]')
    prog_args.add_argument('--preprocess', default=False, action='store_true',
                           help='Whether to preprocess the input files for '
                                'validation purposes. [Default: False]')
    prog_args.add_argument('-v', '--verbosity', default=1,
                           type=int, action='store',
                           help='Verbosity level for level of output.  A '
                                'value of 0 means no output, and a value '
                                'greater than 0 means more output detail. '
                                '[Default: 1]')
    prog_args.add_argument('-l', '--limit', default=0, type=int, action='store',
                           help='The limit on the number of time-series '
                                'files per processor to write.  Useful when '
                                'debugging.  A limit of 0 means write all '
                                'output files. [Default: 0]')
    prog_args.add_argument('--no-check', action='store_false', dest="check",
                           help='Whether to check the output files after '
                                'creation. [Default: False]')

    # Tuning Arguments
    tuning_args = parser.add_argument_group('Tuning',
                                            'Specify whether to generate '
                                            'or save tuning data')
    tuning_args.add_argument('--tune', default=False, action='store_true',
                             help='Enable tuning of performace')
    tuning_args.add_argument('--with-tuning-file', type=str,
                             help='use tuning file for partitioning '
                                  'variables among processes')
    tuning_args.add_argument('--save-tuning-file', type=str,
                             help='save tuning file for partitioning '
                                  'variables among processes')

    # Parse the CLI options and assemble the Reshaper inputs
    return parser.parse_args()


#==============================================================================
# Main script
#==============================================================================
def main(args):

    # Simple MPI communicator
    simplecomm = create_comm(serial=args.serial)

    # Only the manager process needs to write the configuration to the screen
    # and generate the list of input files.
    if simplecomm.is_manager():
        print "="*80
        print "Main Options"

        if (not args.files):
            print "    Case name        : {0}".format(args.case)
            print "    Model            : {0}".format(args.model)
            print "    Start year       : {0}".format(args.start)
            print "    End year         : {0}".format(args.end)
        else:
            print "    Number of input files : {0}".format(len(args.files))

        print
        print "Output settings"
        print "    Append?          : {0}".format(args.append)
        print "    Once             : {0}".format(args.once)
        print "    Skip Existing?   : {0}".format(args.skip_existing)
        print "    Overwrite?       : {0}".format(args.overwrite)
        print "    Compression      : {0}".format(args.deflate)
        print "    Output directory : {0}".format(args.odir)

        print
        print 'Program Options'
        print "    Backend          : {0}".format(args.backend)
        print "    Serial?          : {0}".format(args.serial)
        print "    Time Code?       : {0}".format(args.timecode)
        print "    Preprocess?      : {0}".format(args.preprocess)
        print "    Verbosity        : {0}".format(args.verbosity)

        print
        print "Tuning"
        print "    Tune this run?   : {0}".format(args.tune)
        print "    Save tuning file : {0}".format(args.save_tuning_file)
        print "    With tuning file : {0}".format(args.with_tuning_file)

        print "="*80

        if not args.files:

            total_years = args.end - args.start + 1
            years = range(args.start, args.end + 1)

            # A mapping between model names and file corresponding file names
            mtypes = {"atm":"cam2", "lnd":"clm2", "ocn":"pop", "ice":"cice"}
            freqtypes = {"atm":"h0", "lnd":"h0", "ocn":"h", "ice":"h"}

            comp_direc = ospath.join(args.root, args.model, "hist")

            # Generating the list of files that need to be worked on
            list_of_files = []
            for year in years:
                pattern = "{0}/{1}.{2}.{3}.{4:04d}*.nc".format(
                    comp_direc, args.case, mtypes[args.model],
                    freqtypes[args.model], year)
                list_of_files.extend(glob.glob(pattern))

            list_of_files.sort()

        else:
            list_of_files = args.files

        print "+" + "-"*78 + "+"
        print "|" + ("Number of files to operate upon: {0:3d}".format(
            len(list_of_files))).center(78) + "|"
        print "+" + "-"*78 + "+"

    else:
        list_of_files = None

    # We broadcast the list of files to all processes
    list_of_files = simplecomm._comm.bcast(list_of_files, root=0)

    # output_prefix = "tseries_{0}_{1}.".format(args.start, args.end)
    output_prefix = "tseries."

    # Create the input object for the Reshaper
    spec = create_specifier(infiles=list_of_files,
                            ncfmt="netcdf4c",
                            deflate=args.deflate,
                            prefix=output_prefix,
                            suffix=".nc",
                            outdir=args.odir,
                            metadata=args.metadata)

    # Create the PyReshaper object
    reshpr = create_reshaper(spec,
                             serial=args.serial,
                             verbosity=args.verbosity,
                             skip_existing=args.skip_existing,
                             overwrite=args.overwrite,
                             append=args.append,
                             once=args.once,
                             simplecomm=simplecomm,
                             backend=args.backend,
                             timecode=args.timecode,
                             sort_files=False,
                             preprocess=args.preprocess,
                             check=args.check)

    # Set tuning parameters
    if args.tune:
        reshpr.tune = True
        if args.save_tuning_file:
            reshpr.save_tuning_data = True
            reshpr.output_tuning_file = args.save_tuning_file

    if args.with_tuning_file:
        reshpr.use_tuning_data = True
        reshpr.input_tuning_file = args.with_tuning_file

    # Run the conversion (slice-to-series) process
    reshpr.convert(output_limit=args.limit)

    # Print timing diagnostics
    reshpr.print_diagnostics()


#==============================================================================
# Command-line Opeartion
#==============================================================================
if __name__ == '__main__':
    args = cli()

    # Checking for output directories
    if not ospath.isdir(args.odir):
        raise ValueError("Invalid output directory {0}".format(args.odir))

    main(args)
