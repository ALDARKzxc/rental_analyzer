import unittest

from app.parser.ostrovok_parser import OstrovokParser


class OstrovokMetadataHelperTests(unittest.TestCase):
    def setUp(self):
        self.parser = OstrovokParser()
        self.parser._proxy = None

    def test_capacity_from_room_name_picks_correct_number(self):
        self.assertEqual(self.parser._capacity_from_room_name("Двухместный коттедж"), 2)
        self.assertEqual(self.parser._capacity_from_room_name("Четырёхместный коттедж с мангалом"), 4)
        self.assertEqual(self.parser._capacity_from_room_name("четырехместный номер"), 4)
        self.assertEqual(self.parser._capacity_from_room_name("Шестиместный дом"), 6)
        self.assertIsNone(self.parser._capacity_from_room_name("Просто комната"))

    def test_meta_address_prefers_jsonld_street_address(self):
        jsonld = {
            "@type": "Hotel",
            "name": "Коттедж",
            "address": {
                "@type": "PostalAddress",
                "streetAddress": "ул. Эльбрусская, 15А, Терскол",
                "addressLocality": "Терскол",
            },
        }
        self.assertEqual(
            self.parser._meta_address(None, jsonld),
            "ул. Эльбрусская, 15А, Терскол",
        )

    def test_meta_address_falls_back_to_hotel_location(self):
        hotel = {
            "location": {
                "address": "ул. Ленина, 10, Москва",
            }
        }
        self.assertEqual(
            self.parser._meta_address(hotel, None),
            "ул. Ленина, 10, Москва",
        )

    def test_meta_address_returns_none_when_missing(self):
        self.assertIsNone(self.parser._meta_address(None, None))
        self.assertIsNone(self.parser._meta_address({"location": {}}, {"address": {}}))

    def test_meta_guest_capacity_picks_max_from_room_groups(self):
        hotel = {
            "roomGroups": [
                {"nameStruct": {"mainName": "Двухместный коттедж"}},
                {"nameStruct": {"mainName": "Четырёхместный коттедж"}},
                {"nameStruct": {"mainName": "Шестиместный коттедж"}},
            ],
            "apartmentsInfo": {"capacity": 0},
        }
        self.assertEqual(self.parser._meta_guest_capacity(hotel, ""), 6)

    def test_meta_guest_capacity_uses_apartments_info_when_room_names_missing(self):
        hotel = {
            "roomGroups": [],
            "apartmentsInfo": {"capacity": 8},
        }
        self.assertEqual(self.parser._meta_guest_capacity(hotel, ""), 8)

    def test_meta_guest_capacity_html_fallback(self):
        html = """
        <script>
            window.__DATA__ = {
                "room_capacity": 8,
                "deposit": 4000,
                "guestCapacity": 6
            };
        </script>
        """
        self.assertEqual(self.parser._meta_guest_capacity(None, html), 8)

    def test_picks_worldota_image_over_service_assets(self):
        candidates = [
            "https://ostrovok.ru/static/logo.svg",
            "https://cdn.worldota.net/t/640x400/content/aa/bb/cc.jpeg",
            "https://ostrovok.ru/static/icon.png",
        ]
        image_url = self.parser._pick_ostrovok_image(
            candidates,
            "https://ostrovok.ru/hotel/russia/terskol/mid13310009/cottage_with_barbecue/",
        )
        self.assertEqual(
            image_url,
            "https://cdn.worldota.net/t/640x400/content/aa/bb/cc.jpeg",
        )

    def test_meta_image_aggregates_jsonld_and_hotel_images(self):
        jsonld = {
            "@type": "Hotel",
            "image": [
                "https://cdn.worldota.net/t/1024x768/content/aa/bb/main.jpg",
                {"url": "https://cdn.worldota.net/t/640x400/content/aa/bb/extra.jpg"},
            ],
        }
        hotel = {
            "images": [
                {"src": "https://cdn.worldota.net/t/640x400/content/cc/dd/room.jpg"},
            ]
        }
        image_url = self.parser._meta_image(
            hotel,
            jsonld,
            "",
            "https://ostrovok.ru/hotel/russia/terskol/mid1/test/",
        )
        self.assertIn("cdn.worldota.net", image_url)

    def test_meta_image_falls_back_to_og_image(self):
        html = (
            '<meta property="og:image" '
            'content="https://cdn.worldota.net/t/1024x768/content/og/image.jpg">'
        )
        image_url = self.parser._meta_image(
            None, None, html, "https://ostrovok.ru/hotel/russia/terskol/mid1/test/",
        )
        self.assertEqual(
            image_url,
            "https://cdn.worldota.net/t/1024x768/content/og/image.jpg",
        )

    def test_clean_ostrovok_address_filters_map_button(self):
        self.assertIsNone(self.parser._clean_ostrovok_address("Показать на карте"))
        self.assertEqual(
            self.parser._clean_ostrovok_address("ул. Эльбрусская, 15А, Терскол"),
            "ул. Эльбрусская, 15А, Терскол",
        )


if __name__ == "__main__":
    unittest.main()
