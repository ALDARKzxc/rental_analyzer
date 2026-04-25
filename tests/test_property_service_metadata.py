import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.backend import property_service


class PropertyServiceMetadataTests(unittest.IsolatedAsyncioTestCase):
    async def test_enrich_property_metadata_updates_auto_title_and_card_fields(self):
        prop = SimpleNamespace(
            id=1,
            url="https://example.com/object",
            is_active=True,
            title="example.com",
            address=None,
            guest_capacity=None,
            preview_path=None,
        )
        captured_updates = {}

        async def fake_get_by_id(_property_id):
            return prop

        async def fake_update(_property_id, **kwargs):
            captured_updates.update(kwargs)
            for key, value in kwargs.items():
                setattr(prop, key, value)
            return prop

        async def fake_fetch_metadata(_dispatcher, url):
            self.assertEqual(url, prop.url)
            return {
                "title": "Апартаменты у моря",
                "address": "ул. Ленина, 10",
                "guest_capacity": 4,
                "image_url": "https://example.com/image.jpg",
            }

        async def fake_cache_preview_image(property_id, image_url):
            self.assertEqual(property_id, prop.id)
            self.assertEqual(image_url, "https://example.com/image.jpg")
            return r"C:\tmp\property_1.jpg"

        with (
            patch.object(property_service.PropertyRepository, "get_by_id", side_effect=fake_get_by_id),
            patch.object(property_service.PropertyRepository, "update", side_effect=fake_update),
            patch("app.parser.dispatcher.ParserDispatcher.fetch_metadata", new=fake_fetch_metadata),
            patch("app.backend.property_service._cache_preview_image", side_effect=fake_cache_preview_image),
        ):
            result = await property_service.enrich_property_metadata(
                prop.id,
                allow_title_update=True,
            )

        self.assertIs(result, prop)
        self.assertEqual(captured_updates["title"], "Апартаменты у моря")
        self.assertEqual(captured_updates["address"], "ул. Ленина, 10")
        self.assertEqual(captured_updates["guest_capacity"], 4)
        self.assertEqual(captured_updates["preview_path"], r"C:\tmp\property_1.jpg")

    async def test_enrich_property_metadata_keeps_manual_title(self):
        prop = SimpleNamespace(
            id=2,
            url="https://example.com/object-2",
            is_active=True,
            title="Моё название",
            address=None,
            guest_capacity=None,
            preview_path=None,
        )
        captured_updates = {}

        async def fake_get_by_id(_property_id):
            return prop

        async def fake_update(_property_id, **kwargs):
            captured_updates.update(kwargs)
            for key, value in kwargs.items():
                setattr(prop, key, value)
            return prop

        async def fake_fetch_metadata(_dispatcher, _url):
            return {
                "title": "Название с сайта",
                "address": "Невский проспект, 15",
                "guest_capacity": 2,
                "image_url": None,
            }

        with (
            patch.object(property_service.PropertyRepository, "get_by_id", side_effect=fake_get_by_id),
            patch.object(property_service.PropertyRepository, "update", side_effect=fake_update),
            patch("app.parser.dispatcher.ParserDispatcher.fetch_metadata", new=fake_fetch_metadata),
            patch("app.backend.property_service._cache_preview_image", return_value=None),
        ):
            result = await property_service.enrich_property_metadata(
                prop.id,
                allow_title_update=False,
            )

        self.assertIs(result, prop)
        self.assertNotIn("title", captured_updates)
        self.assertEqual(captured_updates["address"], "Невский проспект, 15")
        self.assertEqual(captured_updates["guest_capacity"], 2)


if __name__ == "__main__":
    unittest.main()
