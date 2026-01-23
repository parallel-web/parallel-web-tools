"""Runtime hook to configure SSL certificates for PyInstaller bundles."""

import os
import sys


def _configure_ssl():
    """Set SSL_CERT_FILE to the bundled certifi certificates."""
    if getattr(sys, "frozen", False):
        # Running as PyInstaller bundle
        bundle_dir = sys._MEIPASS
        cert_path = os.path.join(bundle_dir, "certifi", "cacert.pem")
        if os.path.exists(cert_path):
            os.environ["SSL_CERT_FILE"] = cert_path
            os.environ["REQUESTS_CA_BUNDLE"] = cert_path


_configure_ssl()
