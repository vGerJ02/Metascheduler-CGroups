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
            ssh_cmd = self._build_ssh_command(command)
            
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=float(os.getenv('SSH_TIMEOUT', '30'))
            )
            
            if result.returncode != 0 and critical:
                error_msg = result.stderr.strip() or result.stdout.strip()
                raise RuntimeError(
                    f'SSH command failed with exit code {result.returncode}: {error_msg}'
                )
            
            return result.stdout
            
        except subprocess.TimeoutExpired as e:
            error_message = (
                f'SSH command timed out against {self.ip}:{self.port} as '
                f'{os.getenv("SSH_USER")} while running "{command}"'
            )
            if critical:
                raise RuntimeError(error_message) from e
            return error_message
        except Exception as e:
            error_message = (
                f'SSH command failed against {self.ip}:{self.port} as '
                f'{os.getenv("SSH_USER")} (key={os.getenv("SSH_KEY_FILE")}) while running "{command}": {e}'
            )
            if critical:
                raise RuntimeError(error_message) from e
            return error_message

    def send_command_async(self, command: str, on_output=None, on_complete=None) -> None:
        '''
        Send a command to the node asynchronously.
        Args:
            command (str): The command to send.
        '''
        def run_command():
            try:
                ssh_cmd = self._build_ssh_command(command)
                process = subprocess.Popen(
                    ssh_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1
                )
                stdout_lines = []
                stderr_lines = []

                def read_stream(stream, is_stderr: bool):
                    for line in iter(stream.readline, ''):
                        if is_stderr:
                            stderr_lines.append(line)
                        else:
                            stdout_lines.append(line)
                        if on_output:
                            try:
                                on_output(line, is_stderr)
                            except Exception as callback_exc:
                                print(f"Error in on_output callback: {callback_exc}")
                    stream.close()

                stdout_thread = threading.Thread(target=read_stream, args=(process.stdout, False))
                stderr_thread = threading.Thread(target=read_stream, args=(process.stderr, True))
                stdout_thread.start()
                stderr_thread.start()
                try:
                    process.wait(timeout=float(os.getenv('SSH_TIMEOUT', '30')))
                except subprocess.TimeoutExpired:
                    process.kill()
                    raise
                finally:
                    stdout_thread.join()
                    stderr_thread.join()

                stdout = ''.join(stdout_lines)
                stderr = ''.join(stderr_lines)

                print("Command:", command)
                print("Ssh Command:", ssh_cmd)
                print("Return code:", process.returncode)
                print("STDOUT:", stdout)
                print("STDERR:", stderr)

                if on_complete:
                    try:
                        on_complete(process.returncode, stdout, stderr)
                    except Exception as callback_exc:
                        print(f"Error in on_complete callback: {callback_exc}")
            except Exception as e:
                print(f"Error sending command: {e}")
                if on_complete:
                    try:
                        on_complete(1, "", str(e))
                    except Exception as callback_exc:
                        print(f"Error in on_complete callback: {callback_exc}")
                raise e

        thread = threading.Thread(target=run_command)
        thread.start()

    def _build_ssh_command(self, command: str) -> list[str]:
        '''
        Build the SSH command as a list for subprocess.
        Args:
            command (str): The command to execute on the remote host.
        Returns:
            list[str]: The SSH command as a list.
        '''
        ssh_user = os.getenv('SSH_USER')
        ssh_key_file = os.getenv('SSH_KEY_FILE')
        ssh_password = os.getenv('SSH_PASSWORD')
        
        if not ssh_user:
            raise ValueError('SSH_USER environment variable is not set')
        
        # Build SSH command
        ssh_cmd = [
            'ssh',
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', f'ConnectTimeout={os.getenv("SSH_TIMEOUT", "10")}',
            '-o', 'BatchMode=yes',  # Disable password prompts
            '-p', str(self.port),
        ]
        
        # Add key file if provided
        if ssh_key_file:
            key_path = Path(ssh_key_file).expanduser()
            if not key_path.is_file():
                raise FileNotFoundError(
                    f'SSH_KEY_FILE points to missing key file: {key_path}')
            if key_path.suffix == '.pub':
                raise ValueError(
                    f'SSH_KEY_FILE must be a private key, not a public key: {key_path}')
            
            ssh_cmd.extend(['-i', str(key_path)])
        
        # Add user@host
        ssh_cmd.append(f'{ssh_user}@{self.ip}')
        
        # Add the command to execute
        ssh_cmd.append(command)
        
        # Handle password authentication if needed (requires sshpass)
        if ssh_password and not ssh_key_file:
            # Prepend sshpass to the command
            ssh_cmd = ['sshpass', '-p', ssh_password] + ssh_cmd
        
        return ssh_cmd

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
