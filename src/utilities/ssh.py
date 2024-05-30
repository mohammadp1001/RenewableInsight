#see here for original code https://sftptogo.com/blog/python-sftp/
from urllib.parse import urlparse
import os
import paramiko
from paramiko import RSAKey
from paramiko.py3compat import decodebytes
import logging


class SSH:
    
    def __init__(self, hostname,username,password,hostkey,port=22):
        """Constructor Method"""
        # Set connection object to None (initial value)
      
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.sftp = None
        self.hostkey = paramiko.RSAKey(data = paramiko.py3compat.decodebytes(hostkey.encode("ascii")))
        self.connection = paramiko.SSHClient()
        if self.hostkey is not None:
            # manually add the server's host key
            self.connection.get_host_keys().add(hostname, 'ssh-rsa', self.hostkey)

        
    def connect(self):
        """Connects to the sftp server and returns the sftp connection object"""

        try:
            # Get the sftp connection object
             self.connection.connect(
                self.hostname,
                port=self.port,
                username=self.username,
                password=self.password,
            )
            
        except Exception as err:
            raise Exception(err)
        finally:
            logging.info(f"Connected to {self.hostname} as {self.username}.")

    def open_sftp(self):
        try:
            self.sftp = self.connection.open_sftp()

        except Exception as err:
            raise Exception(err)    
        finally:
            logging.info(f"Sftp connection to {self.hostname} was established.")     
            
    def disconnect(self):
        """Closes the sftp connection"""
        if self.sftp is not None:
            self.sftp.close()
        self.connection.close()
        logging.error(f"Disconnected from host {self.hostname}")
    
    def listdir(self,remote_path=None):
        
        if self.sftp is not None:
            if remote_path is not None:
                logging.info(self.sftp.listdir(remote_path))
            else:    
                logging.info(self.sftp.listdir('.'))
        else:
            logging.error(f"Please first open the sftp connection to {self.hostname}.")    

    def download_file(self, remote_path, local_path):
        """
        Downloads a file from the remote SFTP server to a local path.
        
        :param sftp: A valid, open SFTP session.
        :param remote_path: The path to the file on the SFTP server.
        :param local_path: The path where the file should be saved locally.
        """
        try:
            # Check if remote_path is a directory
            try:
                self.listdir(remote_path)
                logging.error(f"Error: The specified remote_path '{remote_path}' is a directory.")
                return
            except IOError:
                pass  # remote_path is not a directory, continue

            # Ensure local_path includes the filename, especially if it's currently just a directory
            if os.path.isdir(local_path):
                local_path = os.path.join(local_path, os.path.basename(remote_path))

            self.sftp.get(remote_path, local_path)
            logging.info(f"File successfully downloaded to {local_path}")
        except Exception as e:
           logging.error(f"Failed to download file: {e}")
            



