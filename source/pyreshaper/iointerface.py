"""
The module containing the PyReshaper I/O backend utilities

These functions and classes abstract the NetCDF data-file operations, so that
(at the moment) either netcdf4-python or PyNIO can be used for file I/O.

Copyright 2015, UCAR and Deepak Chandan
See the LICENSE.rst file for details
"""

import abc

# Imports for NetCDF Backends
SUPPORTED_BACKENDS = ['nio', 'netcdf']
AVAILABLE_BACKENDS = []

# Try to import PyNIO
try:
    import Nio
    AVAILABLE_BACKENDS.append('nio')
except ImportError:
    pass

# Try to import netcdf4-python
try:
    import netCDF4
    AVAILABLE_BACKENDS.append('netcdf')
except ImportError:
    pass


#==============================================================================
# create_iointerface - Factory Function for IOInterface objects
#==============================================================================
def create_iointerface(iokind, ncformat='netcdf4c', compression=1):
    """
    Factory function for creating IOInterface objects

    Parameters:
        iokind (str): The kind of I/O object to use, either "nio" (for the
            PyNIO library) or "netcdf" (for the netcdf4-python library)
        ncformat (str): The format to use for NetCDF file output, either
            'netcdf' (for NetCDF-Classic) or 'netcdf4' (for NetCDF4 files)
        compression (int): The level of compression to use for generating
            output files.  A level of 0 means uncompressed.
    """
    if not isinstance(iokind, (str, unicode)):
        err_msg = 'I/O interface kind must be declared with a string'
        raise TypeError(err_msg)
    if not isinstance(ncformat, (str, unicode)):
        err_msg = 'NetCDF format must be declared with a string'
        raise TypeError(err_msg)
    if not isinstance(compression, int):
        err_msg = 'NetCDF compression level must be declared with an int'
        raise TypeError(err_msg)

    if iokind not in SUPPORTED_BACKENDS:
        err_msg = 'I/O interface kind {} not supported'.format(iokind)
        raise SystemError(err_msg)
    if iokind not in AVAILABLE_BACKENDS:
        err_msg = 'I/O interface kind {} not available on this system'.format(iokind)
        raise SystemError(err_msg)

    if iokind == 'nio':
        return NioInterface(ncformat=ncformat, compression=compression)
    elif iokind == 'netcdf':
        return NetCDF4Interface(ncformat=ncformat, compression=compression)


#==============================================================================
# IOInterface
#==============================================================================
class IOInterface(object):
    """
    Base class for I/O Backends for the PyReshaper
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def open(self, fname, mode='r'):
        pass


#==============================================================================
# NioInterface for PyNIO
#==============================================================================
class NioInterface(IOInterface):
    """
    I/O Interface class for PyNIO
    """

    def __init__(self, ncformat='netcdf4', compression=1):
        """
        Initializer

        Parameters:
            ncformat (str): The format to use for NetCDF file output, either
                'netcdf' (for NetCDF-Classic) or 'netcdf4' (for NetCDF4 files)
            compression_level (int): The level of compression_level to use for generating
                output files.  A level of 0 means uncompressed.
        """
        if not isinstance(ncformat, (str, unicode)):
            err_msg = 'NetCDF format must be declared with a string'
            raise TypeError(err_msg)
        if not isinstance(compression, int):
            err_msg = 'NetCDF compression_level level must be declared with an int'
            raise TypeError(err_msg)

        # Construct the PyNIO options object
        opt = Nio.options()
        opt.PreFill = False
        opt.CompressionLevel = compression
        if ncformat == 'netcdf':
            opt.Format = 'Classic'
        elif ncformat == 'netcdf4':
            opt.Format = 'NetCDF4Classic'
            opt.CompressionLevel = 0
        elif ncformat == 'netcdf4c':
            opt.Format = 'NetCDF4Classic'

        # Save the PyNIO options object
        self._nio_options = opt

    def open(self, fname, mode='r'):
        """
        Open a file with the given mode

        Parameters:
            fname (str): Filename to open
            mode (str): I/O mode ('r' for read, 'w' for write, etc)

        Returns:
            niofile: A PyNIO file object
        """
        return Nio.open_file(fname, mode)


#==============================================================================
# NetCDF4Interface for netcdf4-python
#==============================================================================
class NetCDF4Interface(IOInterface):
    """
    I/O Interface class for netcdf4-python
    """

    def __init__(self, ncformat='netcdf4', compression=1):
        self._ncformat = ncformat
        self._compression = compression

        # arguments passed onto netCDF.Dataset
        self._netcdf_dataset_options = {}

        # arguments passed onto Dataset.createDimension
        self._netcdf_dim_options = {}

        # arguments passed onto Dataset.createVariable
        self._netcdf_var_options = {}

        # Set the options
        if ncformat == 'netcdf':
            self._netcdf_dataset_options["format"] = "NETCDF3_64BIT"
        elif ncformat == 'netcdf4':
            self._netcdf_dataset_options["format"] = "NETCDF4_CLASSIC"
        elif ncformat == 'netcdf4c':
            self._netcdf_dataset_options["format"] = "NETCDF4"
            self._netcdf_var_options["zlib"] = True
            self._netcdf_var_options["complevel"] = compression

    def open(self, fname, mode='r'):
        """
        Open a file with the given mode

        Parameters:
            fname (str): Filename to open
            mode (str): I/O mode ('r' for read, 'w' for write, etc)

        Returns:
            netCDF.Dataset: A netCDF4 Dataset object
        """
        return netCDF4.Dataset(fname, "r")
