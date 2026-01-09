"""
This module provides classes and methods to launch the PDF Calc application.
PDF Calc analyzes Gray-Scott simulation output and computes the probability
distribution function (PDF) for each 2D slice of the U and V variables.
"""
from jarvis_cd.core.pkg import Application
from jarvis_cd.shell import Exec, MpiExecInfo, PsshExecInfo
from jarvis_cd.shell.process import Rm
import os


class Adios2PdfCalc(Application):
    """
    This class provides methods to launch the PDF Calc application.
    """
    def _init(self):
        """
        Initialize paths
        """
        pass

    def _configure_menu(self):
        """
        Create a CLI menu for the configurator method.
        For thorough documentation of these parameters, view:
        https://github.com/scs-lab/jarvis-util/wiki/3.-Argument-Parsing

        :return: List(dict)
        """
        return [
            {
                'name': 'nprocs',
                'msg': 'Number of processes to spawn',
                'type': int,
                'default': 2,
            },
            {
                'name': 'ppn',
                'msg': 'Processes per node',
                'type': int,
                'default': 16,
            },
            {
                'name': 'input_file',
                'msg': 'Input file from Gray-Scott simulation',
                'type': str,
                'default': None,
            },
            {
                'name': 'output_file',
                'msg': 'Output file for PDF analysis results',
                'type': str,
                'default': None,
            },
            {
                'name': 'nbins',
                'msg': 'Number of bins for PDF calculation',
                'type': int,
                'default': 1000,
            },
            {
                'name': 'output_inputdata',
                'msg': 'Write original variables in output (YES/NO)',
                'type': str,
                'default': 'NO',
            },
        ]

    def _configure(self, **kwargs):
        """
        Converts the Jarvis configuration to application-specific configuration.

        :param kwargs: Configuration parameters for this pkg.
        :return: None
        """
        # Validate required parameters
        if self.config['input_file'] is None:
            raise ValueError('input_file parameter is required for pdf_calc')
        if self.config['output_file'] is None:
            raise ValueError('output_file parameter is required for pdf_calc')

    def start(self):
        """
        Launch the PDF Calc application.

        :return: None
        """
        # Build the pdf_calc command
        pdf_cmd = (f'pdf_calc {self.config["input_file"]} '
                  f'{self.config["output_file"]} '
                  f'{self.config["nbins"]}')

        # Add optional output_inputdata parameter if set to YES
        output_inputdata = str(self.config['output_inputdata']).upper()
        if output_inputdata == 'YES':
            pdf_cmd += f' {output_inputdata}'

        # Execute pdf_calc with MPI
        Exec(pdf_cmd,
             MpiExecInfo(nprocs=self.config['nprocs'],
                         ppn=self.config['ppn'],
                         hostfile=self.hostfile,
                         env=self.mod_env)).run()

    def stop(self):
        """
        Stop a running application.

        :return: None
        """
        pass

    def clean(self):
        """
        Destroy all data for the PDF Calc application.

        :return: None
        """
        if self.config['output_file']:
            print(f'Removing {self.config["output_file"]}')
            Rm(self.config['output_file'], PsshExecInfo(hostfile=self.hostfile)).run()
