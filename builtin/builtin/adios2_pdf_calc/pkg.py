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
        self.adios2_xml_path = f'{self.shared_dir}/adios2.xml'
        self.adios2_xml_runtime = f'{self.private_dir}/adios2.xml'  # Copy to private dir for execution

        # Ensure PATH is available for MPI detection
        # This runs every time the package is loaded (including at start time)
        import os
        if 'PATH' not in self.env:
            system_path = os.environ.get('PATH', '')
            if system_path:
                self.env['PATH'] = system_path
                self.mod_env['PATH'] = system_path

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
            {
                'name': 'wait_for_producer',
                'msg': 'Wait for producer to complete before starting',
                'type': bool,
                'default': True,
            },
            {
                'name': 'engine',
                'msg': 'ADIOS2 engine to use for reading',
                'choices': ['bp5', 'sst'],
                'type': str,
                'default': 'bp5',
            },
        ]

    def _configure(self, **kwargs):
        """
        Converts the Jarvis configuration to application-specific configuration.

        :param kwargs: Configuration parameters for this pkg.
        :return: None
        """
        # Ensure pdf_calc binary location is in PATH
        # This is needed for MPI execution to find the binary
        import os
        pdf_calc_bin_dir = '/workspace/external/iowarp-gray-scott/build/bin'
        if os.path.exists(pdf_calc_bin_dir):
            # If PATH doesn't exist in our env, initialize it from system PATH
            if 'PATH' not in self.env:
                system_path = os.environ.get('PATH', '')
                if system_path:
                    self.env['PATH'] = system_path
                    self.mod_env['PATH'] = system_path
            # Now prepend our bin directory
            self.prepend_env('PATH', pdf_calc_bin_dir)

        # Validate required parameters
        if self.config['input_file'] is None:
            raise ValueError('input_file parameter is required for pdf_calc')
        if self.config['output_file'] is None:
            raise ValueError('output_file parameter is required for pdf_calc')

        # Copy ADIOS2 XML configuration based on engine type
        print(f"Using engine {self.config['engine']} for pdf_calc")
        if self.config['engine'].lower() == 'sst':
            # Use SST configuration for streaming
            self.copy_template_file(f'{self.pkg_dir}/config/sst.xml',
                                self.adios2_xml_path)
        else:
            # Use BP5 configuration (default)
            self.copy_template_file(f'{self.pkg_dir}/config/adios2.xml',
                                self.adios2_xml_path)

    def start(self):
        """
        Launch the PDF Calc application.

        :return: None
        """
        import shutil
        import time

        # Copy ADIOS2 XML to working directory where pdf_calc will look for it
        working_dir = os.path.dirname(self.config['input_file'])
        runtime_xml = os.path.join(working_dir, 'adios2.xml')
        shutil.copy(self.adios2_xml_path, runtime_xml)
        print(f"Copied ADIOS2 config to {runtime_xml}")

        # If wait_for_producer is enabled, handle differently for SST vs BP5
        if self.config.get('wait_for_producer', True):
            if self.config['engine'].lower() == 'sst':
                # For SST, just wait a fixed time for producer to start streaming
                print("Waiting 10 seconds for SST producer to initialize...")
                time.sleep(10)
            else:
                # For BP5, wait for file to exist
                print("Waiting for producer to create output file...")
                wait_time = 0
                max_wait = 60
                while wait_time < max_wait:
                    if os.path.exists(self.config['input_file']):
                        print(f"Output file found after {wait_time} seconds")
                        # Give a bit more time for the first timestep to be written
                        time.sleep(5)
                        break
                    time.sleep(1)
                    wait_time += 1

                if wait_time >= max_wait:
                    print(f"Warning: Output file not found after {max_wait} seconds, attempting to open anyway...")

        # Build the pdf_calc command with full path
        pdf_calc_bin = os.path.join(working_dir, 'build/bin/pdf_calc')
        pdf_cmd = (f'{pdf_calc_bin} {self.config["input_file"]} '
                  f'{self.config["output_file"]} '
                  f'{self.config["nbins"]}')

        # Add optional output_inputdata parameter if set to YES
        output_inputdata = str(self.config['output_inputdata']).upper()
        if output_inputdata == 'YES':
            pdf_cmd += f' {output_inputdata}'

        # Change to working directory before executing
        import os as os_module
        original_cwd = os_module.getcwd()
        os_module.chdir(working_dir)

        # Execute pdf_calc with MPI
        Exec(pdf_cmd,
             MpiExecInfo(nprocs=self.config['nprocs'],
                         ppn=self.config['ppn'],
                         hostfile=self.hostfile,
                         env=self.mod_env)).run()

        # Restore original directory
        os_module.chdir(original_cwd)

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
