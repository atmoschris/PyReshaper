"""
The module containing the PyReshaper configuration specification class

This is a configuration specification class, through which the input to
the PyReshaper code is specified.  Currently all types of supported
operations for the PyReshaper are specified with derived dypes of the
Specification class.

Copyright 2015, University Corporation for Atmospheric Research
See the LICENSE.rst file for details
"""

# Built-in imports
from os import path as ospath


#==============================================================================
# create_specifier factory function
#==============================================================================
def create_specifier(**kwargs):
    """
    Factory function for Specifier class objects.  Defined for convenience.

    Parameters:
        kwargs (dict): Optional arguments to be passed to the newly created
            Specifier object's constructor.

    Returns:
        Specifier: An instantiation of the type of Specifier class desired.
    """
    return Specifier(**kwargs)


#==============================================================================
# Specifier Base Class
#==============================================================================
class Specifier(object):

    """
    Time-slice to Time-series Convertion Specifier

    This class acts as a container for the various input data needed
    by the Reshaper to perform the time-slice to time-series operation.
    """

    def __init__(self,
                 infiles=[],
                 ncfmt='netcdf4c',
                 deflate=3,
                 prefix='tseries.',
                 suffix='.nc',
                 outdir=None,
                 metadata=[],
                 **kwargs):
        """
        Initializes the internal data with optional arguments.

        The time-series output files are named according to the
        convention:

            output_file_name = outdir + prefix + variable_name + suffix

        The output_file_name should be a full-path filename.

        Parameters:
            infiles (list): List of full-path input filenames
            ncfmt (str): String specifying the NetCDF
                data format ('netcdf','netcdf4','netcdf4c')
            deflate (int): Compression level
            prefix (str): String specifying the full-path prefix common
                to all time-series output files
            suffix (str): String specifying the (base filename) suffix common
                to all time-series output files
            outdir (str): Output directory
            metadata (list): List of variable names specifying the
                variables that should be included in every
                time-series output file
            kwargs (dict): Optional arguments describing the
                Reshaper run
        """

        # The list of input (time-slice) NetCDF files (absolute paths)
        self.input_file_list = infiles

        # The string specifying the NetCDF file format for output
        self.netcdf_format = ncfmt

        # Compression level for the output files
        self.netcdf_deflate = deflate

        # The directory where the output files will be placed
        self.output_directory = outdir

        # The common prefix to all output files (following the rule:
        #   outdir + prefix + variable_name + suffix)
        self.output_file_prefix = prefix

        # The common suffix to all output files (following the rule:
        #   outdir + prefix + variable_name + suffix)
        self.output_file_suffix = suffix

        # List of time-variant variables that should be included in all
        #  output files.
        self.time_variant_metadata = metadata

        # Optional arguments associated with the reshaper operation
        self.options = kwargs

    def validate(self):
        """
        Perform self-validation of internal data
        """

        # Validate types
        self.validate_types()

        # Validate values
        self.validate_values()

    def validate_types(self):
        """
        Method for checking the types of the Specifier data.

        This method is called by the validate() method.
        """

        # Validate the type of the input file list
        if not isinstance(self.input_file_list, (list, tuple)):
            err_msg = "Input file list must be a list"
            raise TypeError(err_msg)

        # Validate that each input file name is a string
        for ifile_name in self.input_file_list:
            if not isinstance(ifile_name, (str, unicode)):
                err_msg = "Input file names must be given as strings"
                raise TypeError(err_msg)

        # Validate the netcdf format string
        if not isinstance(self.netcdf_format, (str, unicode)):
            err_msg = "NetCDF format must be given as a string"
            raise TypeError(err_msg)

        # Validate the output file prefix
        if self.output_directory is not None:
            if not isinstance(self.output_directory, (str, unicode)):
                err_msg = "Output directory must be given as a string"
                raise TypeError(err_msg)

        # Validate the output file prefix
        if not isinstance(self.output_file_prefix, (str, unicode)):
            err_msg = "Output file prefix must be given as a string"
            raise TypeError(err_msg)

        # Validate the output file suffix
        if not isinstance(self.output_file_suffix, (str, unicode)):
            err_msg = "Output file suffix must be given as a string"
            raise TypeError(err_msg)

        # Validate the type of the time-variant metadata list
        if not isinstance(self.time_variant_metadata, (list, tuple)):
            err_msg = "Time-variant metadata must be a list"
            raise TypeError(err_msg)

        # Validate the type of each time-variant metadata variable name
        for var_name in self.time_variant_metadata:
            if not isinstance(var_name, (str, unicode)):
                err_msg = "Time-variant metadata variable names must be " + \
                          "given as strings"
                raise TypeError(err_msg)

    def validate_values(self):
        """
        Method to validate the values of the Specifier data.

        This method is called by the validate() method.

        We impose the (somewhat arbitrary) rule that the Specifier
        should not validate values what require "cracking" open the
        input files themselves.  Hence, we validate values that can
        be checked without any PyNIO file I/O (including reading the
        header information).

        This method will correct some input if it is safe to do so.
        """

        # Make sure there is at least 1 input file given
        if len(self.input_file_list) == 0:
            err_msg = "There must be at least one input file given."
            raise ValueError(err_msg)

        # Validate that each input file exists and is a regular file
        for ifile_name in self.input_file_list:
            if not ospath.isfile(ifile_name):
                err_msg = "Input file {0} is not a regular file".format(ifile_name)
                raise ValueError(err_msg)

        # Validate the value of the netcdf format string
        valid_formats = ['netcdf', 'netcdf4', 'netcdf4c']
        if self.netcdf_format not in valid_formats:
            err_msg = "Invalid output NetCDF file format {0}".format(self.netcdf_format)
            raise ValueError(err_msg)

        # Validate that format is compatible with compression
        if self.netcdf_format != "netcdf4c":
            if self.netcdf_deflate != 0:
                print "Warning: Ignoring compression because output ",
                print "file-format is '{0}'".format(self.netcdf_format)

        # Validate the value of the output directory (must exist)
        if self.output_directory is not None:
            # Validate the output file directory
            invalid_chars = "/~!@#$%^&*():;?<>`+=[]{}"
            for iv in invalid_chars:
                if iv in self.output_file_prefix:
                    err_msg = "Character {0} not allowed in filename".format(iv)
                    err_msg += " prefix when specifying output directory"
                    raise ValueError(err_msg)
            if not ospath.isdir(self.output_directory):
                err_msg = "Invalid output directory {0}".format(self.output_directory)
                raise ValueError(err_msg)
            self.output_file_prefix = ospath.join(self.output_directory,
                                                  self.output_file_prefix)

        # Validate the output file suffix string (should end in .nc)
        if (self.output_file_suffix[-3:] != '.nc'):
            self.output_file_suffix += '.nc'


#==============================================================================
# Command-line Operation
#==============================================================================
if __name__ == '__main__':
    pass
