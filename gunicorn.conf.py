# Gunicorn configuration for Render
# timeout ארוך כי סריקות GBP רצות בthread ברקע עם subprocess שלוקח זמן
timeout = 300
workers = 1
threads = 2
worker_class = "gthread"
