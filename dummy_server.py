import http.server
import socketserver
import json

class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        print(f"GET {self.path}")
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "ok"}')

    def do_POST(self):
        print(f"POST {self.path}")
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        print(f"Body: {post_data.decode('utf-8')}")
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"poToken": "dummy_po", "visitorData": "dummy_visitor"}')

with socketserver.TCPServer(("", 4416), Handler) as httpd:
    print("Serving on port 4416")
    httpd.serve_forever()
