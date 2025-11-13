#!/usr/bin/env python3
"""
Mock GitLab Internal API Server for Benchmarking

This server mocks GitLab's internal API endpoints that some of Gitaly's
RPCs need to contact for permission checks. It returns success for all
requests, allowing RPCs to function in a standalone Gitaly
benchmarking environment without requiring a full GitLab Rails deployment.
"""

import json
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MockGitLabAPIHandler(BaseHTTPRequestHandler):
    """Handler for GitLab internal API requests."""

    def log_message(self, format, *args):
        logger.info("%s - %s" % (self.address_string(), format % args))

    def send_json_response(self, data, status=200):
        """Send a JSON response."""
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))

    def do_POST(self):
        """Handle POST requests."""
        parsed_path = urlparse(self.path)
        path = parsed_path.path

        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else ''

        logger.info(f"POST {path}")

        if path == '/api/v4/internal/allowed':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status":true}')

        elif path == '/api/v4/internal/pre_receive':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"reference_counter_increased":true}')

        elif path == '/api/v4/internal/post_receive':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"reference_counter_decreased":true,"messages":[]}')
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"error":"not found"}')

    def do_GET(self):
        """Handle GET requests."""
        parsed_path = urlparse(self.path)
        path = parsed_path.path

        logger.info(f"GET {path}")

        if path == '/api/v4/internal/check':
            response = {
                "api_version": "v4",
                "gitlab_version": "mock",
                "gitlab_rev": "mock",
                "redis": True
            }
            self.send_json_response(response)

# The GitLab URL is set to http://127.0.0.1:3001 in the Gitaly config.toml file.
def run_server(host='127.0.0.1', port=3001):
    """Start the mock API server."""
    server_address = (host, port)
    httpd = HTTPServer(server_address, MockGitLabAPIHandler)

    logger.info(f"Mock GitLab API server starting on {host}:{port}")
    logger.info("This server will respond with success to all permission checks")
    logger.info("Press Ctrl+C to stop")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    finally:
        httpd.server_close()
        logger.info("Server shutdown complete")


if __name__ == '__main__':
    run_server()


