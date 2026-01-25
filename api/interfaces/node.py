import os
import subprocess
import threading
from pathlib import Path
from icmplib import ping


class Node:
    '''
    Interface for a node.
    '''
    id_: int
    ip: str
    port: int
    is_alive: bool | None

    def __init__(self, id_: int, ip: str, port: int) -> None:
        '''
        Constructor.
        '''
        self.id_ = id_
        self.ip = ip
        try:
            self.port = int(port)
        except ValueError as exc:
            raise ValueError(f'Invalid port "{port}" for node {ip}') from exc
        self.is_alive = self._is_alive()

    def __str__(self) -> str:
        '''
        String representation of the node.
        '''
        return f'ID: {self.id_}, IP: {self.ip}, Port: {self.port}'

    def send_command(self, command: str, critical=True) -> str:
        '''
        Send a command to the node.
        Args:
            command (str): The command to send.
            critical (bool): Whether to raise an exception on failure.
        Returns:
            str: The response from the node.
        '''
        try:
            with self._get_connection() as conn:
                result = conn.run(command, hide=True)
                return result.stdout
        except Exception as e:
            error_message = (
                f'SSH command failed against {self.ip}:{self.port} as '
                f'{os.getenv("SSH_USER")} (key={os.getenv("SSH_KEY_FILE")}) while running "{command}": {e}'
            )
            if critical:
                raise RuntimeError(error_message) from e
            return error_message

    def send_command_async(self, command: str) -> None:
        '''
        Send a command to the node asynchronously.
        Args:
            command (str): The command to send.
        '''

        def run_command():
            try:
                with self._get_connection() as conn:
                    conn.run(command, hide=True)
            except Exception as e:
                raise e

        thread = threading.Thread(target=run_command)
        thread.start()

    def _get_connection(self) -> Connection:
        '''
        Build the SSH connection with configurable timeouts to avoid banner
        read errors on slow or busy nodes.
        '''
        ssh_key_file = os.getenv('SSH_KEY_FILE')
        connect_kwargs = {
            'banner_timeout': float(os.getenv('SSH_BANNER_TIMEOUT', '30')),
            'auth_timeout': float(os.getenv('SSH_AUTH_TIMEOUT', '30')),
            'timeout': float(os.getenv('SSH_TIMEOUT', '10')),
            'allow_agent': True,
            'look_for_keys': True,
        }
        ssh_password = os.getenv('SSH_PASSWORD')
        if ssh_password:
            connect_kwargs['password'] = ssh_password
        if ssh_key_file:
            key_path = Path(ssh_key_file).expanduser()
            if not key_path.is_file():
                raise FileNotFoundError(
                    f'SSH_KEY_FILE points to missing key file: {key_path}')
            if key_path.suffix == '.pub':
                raise ValueError(
                    f'SSH_KEY_FILE must be a private key, not a public key: {key_path}')
            connect_kwargs['key_filename'] = str(key_path)
            passphrase = os.getenv('SSH_KEY_PASSPHRASE')
            if passphrase:
                connect_kwargs['passphrase'] = passphrase
        return Connection(
            self.ip,
            port=self.port,
            user=os.getenv('SSH_USER'),
            connect_kwargs=connect_kwargs,
        )

    def _is_alive(self) -> bool | None:
        '''
        [ROOT REQUIRED]
        Check if the node is alive.
        Returns:
            bool: True if the node is alive, False otherwise.
            None: If the api is not running as root.
        '''
        try:
            return ping(self.ip, count=1).is_alive
        except Exception:
            return None
