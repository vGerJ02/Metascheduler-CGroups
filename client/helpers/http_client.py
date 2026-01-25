from .singleton import Singleton
import requests
from rich import print
from rich.console import Console
from rich.panel import Panel


class HTTP_Client(metaclass=Singleton):
    def __init__(self, url, port):
        self.url = url
        self.port = port
        self.console = Console()

    def _format_request_error(self, e: requests.exceptions.RequestException) -> str:
        if isinstance(e, requests.exceptions.ConnectionError):
            return (
                "[bold red]Error: Connection refused[/bold red]\n"
                "[yellow]Possible reasons:[/yellow]\n"
                "- The server is not running.\n"
                "- The URL or port is incorrect.\n"
                "[cyan]Suggestion:[/cyan] Please check the server status and ensure the correct URL and port are specified."
            )
        if isinstance(e, requests.exceptions.Timeout):
            return (
                "[bold red]Error: Timeout[/bold red]\n"
                "[yellow]Possible reasons:[/yellow]\n"
                "- The server is taking too long to respond.\n"
                "- The server might be overloaded or down.\n"
                "[cyan]Suggestion:[/cyan] Try again later or check the server status."
            )
        return (
            "[bold red]Error: Request exception[/bold red]\n"
            "[yellow]Possible reasons:[/yellow]\n"
            "- An unexpected error occurred.\n"
            "[cyan]Suggestion:[/cyan] Check the request and try again."
        )

    def handle_request_error(self, e: requests.exceptions.RequestException):
        error_message = self._format_request_error(e)
        panel = Panel(
            error_message,
            title="[bold red]Request Error[/bold red]",
            border_style="red",
        )
        self.console.print(panel)
        exit(1)

    def _format_response_error(self, response: requests.Response) -> str:
        if response.status_code == 403:
            return (
                "[bold red]Error: 403 Forbidden[/bold red]\n"
                "[yellow]Possible reasons:[/yellow]\n"
                "- The user does not have permission to perform the operation.\n"
                "[cyan]Suggestion:[/cyan] Check the user permissions and try again."
            )
        try:
            error_detail = response.json().get("detail", response.text)
        except ValueError:
            error_detail = response.text
        return (
            f"[bold red]Error: {response.status_code}[/bold red]\n"
            f"[yellow]Response message:[/yellow] {error_detail}\n"
            f"[cyan]Suggestion:[/cyan] Check the request and try again."
        )

    def handle_response_error(self, response: requests.Response):
        error_message = self._format_response_error(response)
        panel = Panel(
            error_message,
            title="[bold red]Request Error[/bold red]",
            border_style="red",
        )
        self.console.print(panel)
        exit(1)

    def get(self, endpoint, params=None):
        try:
            response = requests.get(
                f"http://{self.url}:{self.port}/{endpoint}", params=params
            )
            if response.status_code == 200:
                return response
            else:
                self.handle_response_error(response)
        except requests.exceptions.RequestException as e:
            self.handle_request_error(e)

    def put(self, endpoint, data, params=None):
        try:
            response = requests.put(
                f"http://{self.url}:{self.port}/{endpoint}", json=data, params=params
            )
            if response.status_code == 200:
                return response
            else:
                self.handle_response_error(response)
        except requests.exceptions.RequestException as e:
            self.handle_request_error(e)

    def post(self, endpoint, data):
        try:
            response = requests.post(
                f"http://{self.url}:{self.port}/{endpoint}", json=data
            )
            if response.status_code in [200, 201]:
                return response
            else:
                self.handle_response_error(response)
        except requests.exceptions.RequestException as e:
            self.handle_request_error(e)

    def post_safe(self, endpoint, data):
        try:
            response = requests.post(
                f"http://{self.url}:{self.port}/{endpoint}", json=data
            )
        except requests.exceptions.RequestException as e:
            return None, self._format_request_error(e)
        if response.status_code in [200, 201]:
            return response, None
        return response, self._format_response_error(response)

    def delete(self, endpoint, params=None):
        try:
            response = requests.delete(
                f"http://{self.url}:{self.port}/{endpoint}", params=params
            )
            if response.status_code == 200:
                return response
            else:
                self.handle_response_error(response)
        except requests.exceptions.RequestException as e:
            self.handle_request_error(e)
