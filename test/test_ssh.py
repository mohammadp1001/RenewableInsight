import unittest
from unittest.mock import patch, MagicMock
from src.SSH import SSH

class TestSSH(unittest.TestCase):

    @patch('src.SSH.paramiko.SSHClient')
    def setUp(self, mock_ssh_client):
        # Mock the RSAKey generation and SSHClient to avoid real connections
        self.mock_ssh_client = mock_ssh_client
        self.hostname = 'example.com'
        self.username = 'user'
        self.password = 'pass'
        self.hostkey = 'AAAAB3NzaC1yc2EAAAADAQABAAABAQC4'
        self.port = 22

        # Setup the SSH instance with mocked data
        self.ssh_instance = SSH(self.hostname, self.username, self.password, self.hostkey, self.port)

    def test_connect(self):
        # Test the connect method
        self.ssh_instance.connect()
        self.mock_ssh_client.return_value.connect.assert_called_with(self.hostname, port=self.port, username=self.username, password=self.password)

    @patch('src.SSH.paramiko.SSHClient')
    def test_open_sftp(self, mock_ssh_client):
        # Test open_sftp method
        self.ssh_instance.connection = MagicMock()
        self.ssh_instance.open_sftp()
        self.assertTrue(self.ssh_instance.connection.open_sftp.called)

    def test_disconnect(self):
        # Test the disconnect method
        self.ssh_instance.connection = MagicMock()
        self.ssh_instance.sftp = MagicMock()
        self.ssh_instance.disconnect()
        self.ssh_instance.sftp.close.assert_called_once()
        self.ssh_instance.connection.close.assert_called_once()

    @patch('src.SSH.paramiko.SSHClient')
    def test_download_file(self, mock_ssh_client):
        mock_sftp = MagicMock()
        mock_sftp.listdir.side_effect = IOError("Not a directory")  # Ensure this matches the behavior of a failed directory check
        self.ssh_instance.connection = MagicMock()
        self.ssh_instance.connection.open_sftp.return_value = mock_sftp
        self.ssh_instance.sftp = mock_sftp
        self.ssh_instance.download_file('remote/path/file.txt', 'local/path/file.txt')
        mock_sftp.get.assert_called_with('remote/path/file.txt', 'local/path/file.txt')

if __name__ == '__main__':
    unittest.main()
