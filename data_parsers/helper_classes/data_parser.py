"""
Module holds the object models for the Happy Hour app
"""
import os
import requests
import time
import json
import logging
import argparse
import contextlib
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

responses_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'responses')

engine = sqlalchemy.create_engine(os.environ.get('HAPPYFINDER_ENGINE'), encoding='utf8')
Session = sessionmaker(engine)


class Base:
    def __init__(self, data):
        self.data = data


class GoogleDetails(Base):
    @property
    def result(self):
        return self.data.get('result', {})

    @property
    def google_id(self):
        return self.result.get('place_id')

    @property
    def address(self):
        return self.result.get('formatted_address')

    @property
    def url(self):
        return self.result.get('website')

    @property
    def phone_number(self):
        return self.result.get('formatted_phone_number')

    @property
    def opening_hours(self):
        return self.result.get('opening_hours', {})

    @property
    def hours(self):
        return self.opening_hours.get('weekday_text')

    @property
    def rating(self):
        return self.result.get('rating')

    @property
    def geometry(self):
        return self.result.get('geometry', {})

    @property
    def location(self):
        return self.geometry.get('location', {})

    @property
    def lat(self):
        return self.location.get('lat')

    @property
    def lng(self):
        return self.location.get('lng')

    @property
    def name(self):
        return self.result.get('name')

    @property
    def price(self):
        return self.result.get('price_level')

    def __repr__(self):
        return "<Google Details: name: {}, lat: {}, lng: {}, rating: {}, hours: {}, phone_number: {}, address: {}>".format(
            self.name, self.lat, self.lng, self.rating, self.hours, self.phone_number, self.address
        )


class FoursquareDetails(Base):
    @property
    def res(self):
        return self.data.get('response', {})

    @property
    def fs_venue_id(self):
        return self.venues.get('id')

    @property
    def category(self):
        if len(self.venues.get('categories', [])):
            return self.venues.get('categories', [])[0].get('shortName', '')
        return None

    @property
    def has_menu(self):
        return True if 'hasMenu' in self.venues else False

    @property
    def venues(self):
        return self.res.get('venues')[0] if self.res.get('venues') else {}

    def __repr__(self):
        return "FS Details: Response: {}".format(self.res)


class FoursquareVenueDetails(Base):
    @property
    def res_detailed(self):
        return self.data.get('response', {})

    @property
    def menu(self):
        return self.res_detailed.get('menu', {})

    @property
    def menus(self):
        return self.menu.get('menus', {})

    @property
    def menu_items(self):
        return self.menus.get('items', [])

    @property
    def happy_hour_string(self):
        try:
            for menu in self.menu_items:
                if 'happy' in self.menu_name(menu) or 'happy' in self.menu_description(menu):
                    return self.menu_description(menu)
                if len(menu.get('entries', '').get('items', [])):
                    for item in menu['entries']['items']:
                        if 'happy' in item.get('name', '').lower():
                            return 'Not Available'

        except AttributeError as e:
            logging.info(e)
            raise

    @staticmethod
    def menu_name(menu):
        return menu.get('name', '').lower()

    @staticmethod
    def menu_description(menu):
        return menu.get('description', '').lower()

    def __repr__(self):
        return "<FS Venue Details: happy_hour_string: {}>".format(
            self.happy_hour_string
        )
