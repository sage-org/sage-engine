# fixtures.py
# Author: Thomas MINIER - MIT License 2017-2019


def index_scan_fixtures():
    """Get fixtures data for testing Index scans"""
    return [
        # spo
        (
            'http://db.uwaterloo.ca/~galuc/wsdbm/City102',
            'http://www.geonames.org/ontology#parentCountry',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country17',
            [
                ('http://db.uwaterloo.ca/~galuc/wsdbm/City102', 'http://www.geonames.org/ontology#parentCountry', 'http://db.uwaterloo.ca/~galuc/wsdbm/Country17')
            ]
        ),
        # sp?
        (
            'http://db.uwaterloo.ca/~galuc/wsdbm/City102',
            'http://www.geonames.org/ontology#parentCountry',
            None,
            [
                ('http://db.uwaterloo.ca/~galuc/wsdbm/City102', 'http://www.geonames.org/ontology#parentCountry', 'http://db.uwaterloo.ca/~galuc/wsdbm/Country17')
            ]
        ),
        # s?o
        (
            'http://db.uwaterloo.ca/~galuc/wsdbm/City102',
            None,
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country17',
            [
                ('http://db.uwaterloo.ca/~galuc/wsdbm/City102', 'http://www.geonames.org/ontology#parentCountry', 'http://db.uwaterloo.ca/~galuc/wsdbm/Country17')
            ]
        ),
        # ?po
        (
            None,
            'http://www.geonames.org/ontology#parentCountry',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country17',
            [
                ('http://db.uwaterloo.ca/~galuc/wsdbm/City102', 'http://www.geonames.org/ontology#parentCountry', 'http://db.uwaterloo.ca/~galuc/wsdbm/Country17'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/City120', 'http://www.geonames.org/ontology#parentCountry', 'http://db.uwaterloo.ca/~galuc/wsdbm/Country17'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/City123', 'http://www.geonames.org/ontology#parentCountry', 'http://db.uwaterloo.ca/~galuc/wsdbm/Country17'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/City206', 'http://www.geonames.org/ontology#parentCountry', 'http://db.uwaterloo.ca/~galuc/wsdbm/Country17'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/City209', 'http://www.geonames.org/ontology#parentCountry', 'http://db.uwaterloo.ca/~galuc/wsdbm/Country17'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/City217', 'http://www.geonames.org/ontology#parentCountry', 'http://db.uwaterloo.ca/~galuc/wsdbm/Country17')
            ]
        ),
        # s??
        (
            'http://db.uwaterloo.ca/~galuc/wsdbm/Review19570',
            None,
            None,
            [
                ('http://db.uwaterloo.ca/~galuc/wsdbm/Review19570', 'http://purl.org/stuff/rev#rating', '"8"'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/Review19570', 'http://purl.org/stuff/rev#reviewer', 'http://db.uwaterloo.ca/~galuc/wsdbm/User84864'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/Review19570', 'http://purl.org/stuff/rev#text', '"depressant Galveston\'s blindfold\'s Janna Occidentals untying motive\'s reestablished insurer\'s weekday\'s myth secularization site"'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/Review19570', 'http://purl.org/stuff/rev#title', '"Annapurna\'s commence"')
            ]
        ),
        # ??o
        (
            None,
            None,
            'http://db.uwaterloo.ca/~galuc/wsdbm/Product10041',
            [
                ('http://db.uwaterloo.ca/~galuc/wsdbm/Offer24200', 'http://purl.org/goodrelations/includes', 'http://db.uwaterloo.ca/~galuc/wsdbm/Product10041'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/Offer35124', 'http://purl.org/goodrelations/includes', 'http://db.uwaterloo.ca/~galuc/wsdbm/Product10041'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/Offer66663', 'http://purl.org/goodrelations/includes', 'http://db.uwaterloo.ca/~galuc/wsdbm/Product10041'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/Purchase13421', 'http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor', 'http://db.uwaterloo.ca/~galuc/wsdbm/Product10041'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/Purchase63338', 'http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor', 'http://db.uwaterloo.ca/~galuc/wsdbm/Product10041'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/User14463', 'http://db.uwaterloo.ca/~galuc/wsdbm/likes', 'http://db.uwaterloo.ca/~galuc/wsdbm/Product10041'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/User27316', 'http://db.uwaterloo.ca/~galuc/wsdbm/likes', 'http://db.uwaterloo.ca/~galuc/wsdbm/Product10041'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/User47637', 'http://db.uwaterloo.ca/~galuc/wsdbm/likes', 'http://db.uwaterloo.ca/~galuc/wsdbm/Product10041'),
                ('http://db.uwaterloo.ca/~galuc/wsdbm/User66340', 'http://db.uwaterloo.ca/~galuc/wsdbm/likes', 'http://db.uwaterloo.ca/~galuc/wsdbm/Product10041')
            ]
        )
    ]
