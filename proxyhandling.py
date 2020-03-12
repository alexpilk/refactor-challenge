import random

import numpy as np
import pymongo
from pymongo import ReplaceOne


class FailedAfterRetries(Exception):
    pass


class DBProxyHandler:

    def __init__(self, db):
        self.db = db

    def upload(self, proxies):
        requests = [
            ReplaceOne({"address": item}, {"address": item, "successful_job_completion": 0}, upsert=True) for
            item in
            proxies]
        self.db.proxies.bulk_write(
            requests,
            ordered=False)

    def pick(self, n=1, retries=3):
        if n < 1:
            raise ValueError("you must at least one proxy")

        if retries < 3:
            raise FailedAfterRetries

        try:
            proxies = self._filter_active_proxies(n)
            addresses = [proxy["address"] for proxy in proxies]
            scores = [proxy["successful_job_completion"] for proxy in proxies]
            scores = self._calculate_scores(scores)
            p = self._calculate_probabilities(scores)
            chosen_proxies = np.random.choice(addresses, min(n, len(proxies)), replace=False,
                                              p=p)
            if not proxies:
                raise ValueError("no proxies available!")
            return chosen_proxies[0] if n == 1 else chosen_proxies
        except pymongo.errors.AutoReconnect:
            print("pymongo error in proxy.pick: could not autoreconnect")
            self.pick(n, retries - 1)

    def _filter_active_proxies(self, n):
        """
        Drops proxies that have not responded after 30 requests.
        """
        return list(self.db.proxies.find({"successful_job_completion": {"$gt": -30}}).limit(min(10000, 1000 * n)))

    def _calculate_scores(self, proxies):
        def translate_score(score):
            return min(max(score, -5), 5) + 5

        return [random.random() * translate_score(score) for score in proxies]

    def _calculate_probabilities(self, scores):
        score_sum = np.sum(scores)
        return np.array(scores) / score_sum if score_sum else None

    def feedback(self, address, counter=1, retries=3):
        if retries < 0:
            raise FailedAfterRetries

        try:
            proxy = self.db.proxies.find_one({"address": address})
            new_score = proxy.get("successful_job_completion", 0) + counter if proxy else counter
            self.db.proxies.update_one({"address": address}, {
                "$set": {"successful_job_completion": new_score}},
                                       upsert=True)
        except pymongo.errors.AutoReconnect:
            print("pymongo error in feedback: could not autoreconnect")
            self.feedback(address, counter, retries - 1)
