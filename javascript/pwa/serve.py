# serve.py
import http.server
import socketserver
import mimetypes

# Add mapping for .mjs files
mimetypes.add_type('application/javascript', '.js')

PORT = 8000
Handler = http.server.SimpleHTTPRequestHandler

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print("Serving at http://localhost:"+str(PORT))
    try:
        print('To close, Press Ctrl+C and Reload/Exit the Webpage...')
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, shutting down.")
    finally:
        httpd.server_close()
