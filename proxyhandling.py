import random
from typing import List, Dict, Callable, Any

import numpy as np
import pymongo
from pymongo import ReplaceOne


class ProxyError(Exception):
    pass


class FailedAfterRetries(Exception):
    pass


def accepts_retries(default_retries: int) -> Callable:
    """
    Causes decorated method to retry in case of pymongo autoconnect error.
    """

    def decorator(func: Callable) -> Callable:

        def wrapper(*args, retries: int = default_retries, **kwargs) -> Any:

            if retries <= 0:
                raise FailedAfterRetries
            try:
                return func(*args, **kwargs)
            except pymongo.errors.AutoReconnect:
                print(f"Retrying {func.__name__}...")
                return wrapper(*args, retries=retries - 1, **kwargs)

        return wrapper

    return decorator


class DBProxyHandler:

    def __init__(self, db: pymongo.collection.Collection):
        self.db = db

    def upload(self, proxies: List[str]) -> None:
        """
        Saves addresses of proxies in the database.
        """
        requests = [
            ReplaceOne({"address": item}, {"address": item, "successful_job_completion": 0}, upsert=True) for
            item in
            proxies]
        self.db.proxies.bulk_write(
            requests,
            ordered=False)

    @accepts_retries(3)
    def pick(self, number_of_proxies: int = 1) -> List[str] or str:
        """
        Picks given number of proxies based on proxy performance and a bit of randomness.
        """
        if number_of_proxies < 1:
            raise ProxyError("You must specify at least one proxy")

        proxies = self._filter_active_proxies(number_of_proxies)
        if not proxies:
            raise ProxyError("No proxies available!")

        addresses = [proxy["address"] for proxy in proxies]
        scores = [proxy["successful_job_completion"] for proxy in proxies]

        probabilities = self._generate_probabilities(scores)
        sample_size = min(number_of_proxies, len(proxies))
        chosen_proxies = np.random.choice(addresses, sample_size, replace=False, p=probabilities)
        return chosen_proxies[0] if number_of_proxies == 1 else chosen_proxies

    def _filter_active_proxies(self, number_of_proxies: int) -> List[Dict]:
        """
        Drops proxies that have not responded after 30 requests.
        """
        limit = min(10000, 1000 * number_of_proxies)
        drop_score = -30
        return list(self.db.proxies.find({"successful_job_completion": {"$gt": drop_score}}).limit(limit))

    @staticmethod
    def _generate_probabilities(scores: List[int]) -> List[float]:
        """
        Translates number of successful/unsuccessful requests to probabilities, that sum up to 1.
        """

        def translate_score(score):
            return min(max(score, -5), 5) + 5

        randomized_scores = [random.random() * translate_score(score) for score in scores]
        score_sum = np.sum(randomized_scores)
        return np.array(randomized_scores) / score_sum if score_sum else None

    @accepts_retries(3)
    def feedback(self, address: str, counter: int = 1) -> None:
        """
        Adds number of successful/unsuccessful requests to given proxy.
        """
        proxy = self.db.proxies.find_one({"address": address})
        new_score = proxy.get("successful_job_completion", 0) + counter if proxy else counter
        self.db.proxies.update_one({"address": address}, {
            "$set": {"successful_job_completion": new_score}},
                                   upsert=True)
