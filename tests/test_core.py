#!/usr/bin/env python3
"""Offline unit tests — no Telegram connection needed."""

import sys, os, asyncio, unittest
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class TestCorrelation(unittest.TestCase):
    """Test signature matching and SHA1 normalization."""

    def test_signatures_match_same_location_and_type(self):
        from models import EventSignature
        from correlation import signatures_match

        a = EventSignature(location="Jenin", region="west bank", event_type="clash")
        b = EventSignature(location="jenin", region="West Bank", event_type="clash")
        score = signatures_match(a, b)
        self.assertGreaterEqual(score, 0.8)

    def test_signatures_match_different_events(self):
        from models import EventSignature
        from correlation import signatures_match

        a = EventSignature(location="Gaza", event_type="strike")
        b = EventSignature(location="Beirut", event_type="statement")
        score = signatures_match(a, b)
        self.assertLess(score, 0.3)

    def test_signatures_match_entity_overlap(self):
        from models import EventSignature
        from correlation import signatures_match

        a = EventSignature(event_type="clash", entities=["Hamas", "IDF"])
        b = EventSignature(event_type="clash", entities=["hamas", "Hezbollah"])
        score = signatures_match(a, b)
        self.assertGreater(score, 0.3)  # type match + partial entity overlap

    def test_sha1_arabic_diacritics_normalization(self):
        """Same word with and without tashkeel should hash identically."""
        from correlation import sha1_event_key

        # كتب   vs   كُتِبَ  (same root, with diacritics)
        bare = sha1_event_key("كتب")
        with_diacritics = sha1_event_key("كُتِبَ")
        self.assertEqual(bare, with_diacritics)

    def test_sha1_strips_digits(self):
        from correlation import sha1_event_key

        a = sha1_event_key("explosion in area 5")
        b = sha1_event_key("explosion in area 12")
        self.assertEqual(a, b)

    def test_looks_urgent_arabic(self):
        from correlation import looks_urgent
        self.assertTrue(looks_urgent("عاجل: انفجار في بيروت"))
        self.assertFalse(looks_urgent("طقس جميل اليوم"))

    def test_looks_urgent_hebrew(self):
        from correlation import looks_urgent
        self.assertTrue(looks_urgent("דחוף: ירי רקטות לעבר הצפון"))
        self.assertFalse(looks_urgent("מזג אוויר נעים"))

    def test_looks_urgent_emoji(self):
        from correlation import looks_urgent
        self.assertTrue(looks_urgent("🚨 breaking news"))
        self.assertTrue(looks_urgent("🔴 alert"))


class TestAIParsing(unittest.TestCase):
    """Test JSON parsing from LLM output."""

    def test_parse_clean_json(self):
        from ai import _parse_json
        result = _parse_json('{"event_type": "strike", "location": "Gaza"}')
        self.assertEqual(result["event_type"], "strike")

    def test_parse_json_with_markdown_fences(self):
        from ai import _parse_json
        raw = '```json\n{"event_type": "rocket"}\n```'
        result = _parse_json(raw)
        self.assertEqual(result["event_type"], "rocket")

    def test_parse_json_with_extra_text(self):
        from ai import _parse_json
        raw = 'Here is the result:\n{"event_type": "clash", "location": "Jenin"}\nDone.'
        result = _parse_json(raw)
        self.assertEqual(result["event_type"], "clash")

    def test_parse_invalid_json(self):
        from ai import _parse_json
        result = _parse_json("this is not json at all")
        self.assertIsNone(result)


class TestAuthority(unittest.TestCase):
    """Test authority scoring logic."""

    def test_get_label_high(self):
        from authority import AuthorityTracker
        # Test static method behavior
        at = AuthorityTracker.__new__(AuthorityTracker)
        self.assertEqual(at.get_label(85), "גבוהה")

    def test_get_label_medium(self):
        from authority import AuthorityTracker
        at = AuthorityTracker.__new__(AuthorityTracker)
        self.assertEqual(at.get_label(65), "בינונית")

    def test_get_label_low(self):
        from authority import AuthorityTracker
        at = AuthorityTracker.__new__(AuthorityTracker)
        self.assertEqual(at.get_label(40), "נמוכה")


class TestModels(unittest.TestCase):
    """Test data model serialization."""

    def test_event_signature_roundtrip(self):
        from models import EventSignature
        sig = EventSignature(
            location="Rafah", region="gaza", event_type="strike",
            entities=["IDF", "Hamas"], keywords=["airstrike"],
            is_urgent=True,
            credibility_indicators={"has_media_reference": True, "cites_named_source": False},
        )
        d = sig.to_dict()
        restored = EventSignature.from_dict(d)
        self.assertEqual(restored.location, "Rafah")
        self.assertEqual(restored.event_type, "strike")
        self.assertTrue(restored.is_urgent)
        self.assertTrue(restored.credibility_indicators["has_media_reference"])

    def test_event_signature_from_incomplete_dict(self):
        from models import EventSignature
        sig = EventSignature.from_dict({"event_type": "other"})
        self.assertIsNone(sig.location)
        self.assertEqual(sig.entities, [])
        self.assertEqual(sig.credibility_indicators, {})


class TestListenerHelpers(unittest.TestCase):
    """Test listener text cleaning."""

    def test_clean_text_strips_urls(self):
        from listener import _clean_text
        result = _clean_text("check https://t.me/channel/123 this")
        self.assertNotIn("t.me", result)

    def test_clean_text_normalizes_whitespace(self):
        from listener import _clean_text
        result = _clean_text("hello    world\n\nnewline")
        self.assertEqual(result, "hello world newline")

    def test_clean_text_strips_arabic_diacritics(self):
        from listener import _clean_text
        bare = _clean_text("كتب")
        with_tashkeel = _clean_text("كُتِبَ")
        self.assertEqual(bare, with_tashkeel)

    def test_is_blocked(self):
        from listener import _is_blocked
        self.assertTrue(_is_blocked("צבע אדום מופעל"))
        self.assertFalse(_is_blocked("חדשות מהשטח"))

    def test_is_blocked_arabic_spam(self):
        from listener import _is_blocked
        self.assertTrue(_is_blocked("تابعونا على قناتنا"))


class TestSenderHelpers(unittest.TestCase):
    """Test sender badge functions."""

    def test_reliability_badge_high(self):
        from sender import _reliability_badge
        self.assertEqual(_reliability_badge(80), "🟢")

    def test_reliability_badge_medium(self):
        from sender import _reliability_badge
        self.assertEqual(_reliability_badge(60), "🟡")

    def test_reliability_badge_low(self):
        from sender import _reliability_badge
        self.assertEqual(_reliability_badge(30), "🔴")

    def test_source_badge(self):
        from sender import _source_badge
        self.assertIn("מאומת", _source_badge(3))
        self.assertIn("חוזר", _source_badge(2))
        self.assertIn("בודד", _source_badge(1))


if __name__ == "__main__":
    unittest.main()
