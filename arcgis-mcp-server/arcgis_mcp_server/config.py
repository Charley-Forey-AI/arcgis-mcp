"""ArcGIS connection config from environment."""

from __future__ import annotations

import os


def get_gis() -> tuple:
    """
    Create and validate a GIS instance from environment.

    Returns (gis, auth_scheme) where auth_scheme is one of:
    profile, username_password, pki, iwa, pro, anonymous.

    Priority order (first match wins):
    1. ARCGIS_PROFILE -> profile
    2. ARCGIS_URL + ARCGIS_USERNAME + ARCGIS_PASSWORD -> username_password (built-in/LDAP/AD)
    3. ARCGIS_URL + ARCGIS_KEY_FILE + ARCGIS_CERT_FILE -> pki (PEM key+cert)
    4. ARCGIS_URL + ARCGIS_CERT_FILE (.pfx) + ARCGIS_CERT_PASSWORD -> pki (PFX)
    5. ARCGIS_URL only -> iwa (Windows auth when available) or anonymous portal
    6. ARCGIS_USE_PRO=1 -> pro (active ArcGIS Pro portal)
    7. else -> anonymous
    """
    from arcgis.gis import GIS

    profile = os.environ.get("ARCGIS_PROFILE", "").strip()
    url = os.environ.get("ARCGIS_URL", "").strip()
    username = os.environ.get("ARCGIS_USERNAME", "").strip()
    password = os.environ.get("ARCGIS_PASSWORD", "").strip()
    key_file = os.environ.get("ARCGIS_KEY_FILE", "").strip()
    cert_file = os.environ.get("ARCGIS_CERT_FILE", "").strip()
    cert_password = os.environ.get("ARCGIS_CERT_PASSWORD", "").strip()
    use_pro = os.environ.get("ARCGIS_USE_PRO", "").strip().lower() in ("1", "true", "yes")

    gis = None
    auth_scheme = "anonymous"

    if profile:
        gis = GIS(profile=profile)
        auth_scheme = "profile"
    elif url and username and password:
        try:
            gis = GIS(url, username, password)
            auth_scheme = "username_password"
        except Exception as e:
            # Okta/SAML orgs often reject username+password; fall back to anonymous so server starts and OAuth/token auth works
            err = str(e).lower()
            if "invalid username or password" in err or "invalid_username_or_password" in err:
                gis = GIS(url) if url else GIS()
                auth_scheme = "anonymous"
            else:
                raise
    elif url and key_file and cert_file:
        # PKI: PEM key + cert
        gis = GIS(url, key_file=key_file, cert_file=cert_file)
        auth_scheme = "pki"
    elif url and cert_file and cert_password:
        # PKI: PFX file; convert to PEM then connect
        try:
            from arcgis.auth.tools import pfx_to_pem
            key_path, cert_path = pfx_to_pem(cert_file, cert_password)
            gis = GIS(url, key_file=key_path, cert_file=cert_path)
            auth_scheme = "pki"
        except Exception as e:
            raise RuntimeError(
                "Failed to use PKI (PFX) for ArcGIS. Check ARCGIS_URL, ARCGIS_CERT_FILE, ARCGIS_CERT_PASSWORD."
            ) from e
    elif url and not username and not password and not key_file and not cert_file and not use_pro:
        # URL only: IWA on Windows when available, or anonymous portal
        gis = GIS(url)
        auth_scheme = "iwa"
    elif use_pro:
        gis = GIS("pro")
        auth_scheme = "pro"
    else:
        gis = GIS()
        auth_scheme = "anonymous"

    # Validate connection
    try:
        _ = gis.properties.portalName
    except Exception as e:
        raise RuntimeError(
            "Failed to connect to ArcGIS. Check ARCGIS_PROFILE, ARCGIS_URL/credentials, "
            "or ARCGIS_USE_PRO. See README Authentication section."
        ) from e

    return (gis, auth_scheme)
