"""Local receiver. Set FAIL_FIRST=2 to retry twice before accepting each event."""

import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

fail_first = int(os.environ.get("FAIL_FIRST", "0"))
attempts = {}


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/webhooks":
            self.send_error(404)
            return

        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        event_id = payload["eventId"]
        attempts[event_id] = attempts.get(event_id, 0) + 1
        status = 503 if attempts[event_id] <= fail_first else 204
        print(f"event={event_id} attempt={attempts[event_id]} HTTP {status}", flush=True)
        self.send_response(status)
        self.end_headers()


HTTPServer(("127.0.0.1", 5090), Receiver).serve_forever()
