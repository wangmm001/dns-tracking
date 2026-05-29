import pytest
from scripts.parking_archive import build_target_url, build_spn_request


class TestBuildTargetURL:
    @pytest.mark.parametrize("domain,expected", [
        ("example.com",     "http://example.com/"),
        ("foo.bar.io",      "http://foo.bar.io/"),
        ("a.b.c.d.example", "http://a.b.c.d.example/"),
    ])
    def test_canonical_root(self, domain, expected):
        assert build_target_url(domain) == expected


class TestBuildSPNRequest:
    def test_method_and_endpoint(self):
        req = build_spn_request("example.com", "AK", "SK")
        assert req.full_url == "https://web.archive.org/save"
        assert req.get_method() == "POST"

    def test_authorization_header_form(self):
        req = build_spn_request("example.com", "AK", "SK")
        # urllib Request stores headers with first-letter-capitalised names
        assert req.get_header("Authorization") == "LOW AK:SK"

    def test_accept_json(self):
        req = build_spn_request("example.com", "AK", "SK")
        assert req.get_header("Accept") == "application/json"

    def test_body_includes_url_and_flags(self):
        req = build_spn_request("example.com", "AK", "SK")
        body = req.data.decode()
        # url-encoded keys
        assert "url=http%3A%2F%2Fexample.com%2F" in body
        assert "capture_outlinks=0" in body
        assert "delay_wb_availability=0" in body

    def test_user_agent_set(self):
        req = build_spn_request("example.com", "AK", "SK")
        ua = req.get_header("User-agent")
        assert ua is not None
        assert "dns-tracking" in ua
