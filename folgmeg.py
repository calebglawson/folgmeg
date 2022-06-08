import logging.handlers
from sys import stdout
from datetime import datetime, timedelta
from pathlib import Path
import random
from os import environ
from typing import Optional

import typer
import tweepy
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import functions as func

from models import Base, ScriptedFollowingStatus, Status, Follower, OrganicFollowing

Log_Format = "%(levelname)s %(asctime)s - %(message)s"
logger = logging.getLogger(__name__)


class FolgMeg:
    def __init__(
            self,
            db_path,
            consumer_key,
            consumer_secret,
            access_token,
            access_token_secret,
            target_inflight_ratio,
            following_sample_size,
            activity_num_days_lookback,
            tweet_sample_size,
            following_followup_num_days,
    ):
        self._target_inflight_ratio = target_inflight_ratio
        self._following_sample_size = following_sample_size
        self._activity_lookback = timedelta(days=activity_num_days_lookback)
        self._tweet_sample_size = tweet_sample_size
        self._following_followup_time = timedelta(days=following_followup_num_days)

        self._db = self._init_db(db_path)

        self._api = tweepy.API(
            tweepy.OAuth1UserHandler(consumer_key, consumer_secret, access_token, access_token_secret),
            wait_on_rate_limit=True
        )

        self._me = self._api.verify_credentials(skip_status=True)

    @staticmethod
    def _init_db(db_path: Path):
        db_path.mkdir(exist_ok=True)

        new = True
        db_path = db_path.joinpath(Path('folgmeg.db'))
        if db_path.exists():
            new = False

        engine = create_engine(f'sqlite:///{db_path}')

        if new:
            Base.metadata.create_all(engine)

        Base.metadata.bind = engine
        db_session = sessionmaker(bind=engine)

        return db_session()

    def run(self):
        self._update_follower_list()
        self._update_organic_following()
        self._identify_candidates()
        self._follow_candidates()
        self._validate_following()

    def _update_follower_list(self):
        self._me = self._api.verify_credentials(skip_status=True)

        if self._db.query(Follower).count() != self._me.followers_count:
            self._db.query(Follower).delete()
            self._db.bulk_save_objects(
                [
                    Follower(id=follower)
                    for follower in tweepy.Cursor(self._api.get_follower_ids, user_id=self._me.id).items()
                ]
            )

            self._db.commit()

    def _update_organic_following(self):
        if self._db.query(OrganicFollowing).count() == 0:
            self._db.bulk_save_objects(
                [
                    OrganicFollowing(id=following)
                    for following in tweepy.Cursor(self._api.get_friend_ids, user_id=self._me.id).items()
                ]
            )

            self._db.commit()

    def _identify_candidates(self):
        in_flight = self._db.query(ScriptedFollowingStatus).filter(
            ScriptedFollowingStatus.status.in_((Status.pending, Status.following))
        ).count()

        if in_flight / self._db.query(Follower).count() < self._target_inflight_ratio:
            organic_following = self._db.query(OrganicFollowing).order_by(func.random()).limit(
                self._following_sample_size
            ).all()

            followers_of_following = []
            for following in organic_following:
                followers_of_following.extend(self._api.get_follower_ids(user_id=following.id, count=100))

            followers_of_following = random.sample(followers_of_following, self._following_sample_size)
            seven_days_ago = datetime.utcnow() - self._activity_lookback
            for follower in followers_of_following:
                # Skip if I'm going to follow them
                if self._db.query(ScriptedFollowingStatus).filter(ScriptedFollowingStatus.id == follower).count() == 1:
                    continue

                # Skip if they follow me
                if self._db.query(Follower).filter(Follower.id == follower).count() == 1:
                    continue

                last_tweets = []

                try:
                    last_tweets.extend([
                        t for t in self._api.user_timeline(
                            user_id=follower,
                            count=self._tweet_sample_size,
                            include_rts=True,
                        )
                        if t.created_at.timestamp() > seven_days_ago.timestamp()
                    ])
                except Exception as e:
                    logger.error(f'Could not retrieve tweets for {follower}: {e}')

                # Skip if they're not active
                if len(last_tweets) == 0:
                    continue

                hours = {h: 0 for h in range(0, 24)}
                for tweet in last_tweets:
                    hour = tweet.created_at.hour
                    hours[hour] = hours[hour] + 1

                # Is actually earliest, most common
                most_active_hour = max(hours, key=hours.get)

                tomorrow = datetime.utcnow() + timedelta(days=1)
                follow_time = tomorrow.replace(hour=most_active_hour)

                self._db.add(
                    ScriptedFollowingStatus(
                        id=follower,
                        status=Status.pending,
                        next_due=follow_time
                    )
                )
                self._db.commit()

    def _follow_candidates(self):
        due_pending = self._db.query(ScriptedFollowingStatus).filter(
            and_(
                ScriptedFollowingStatus.status == Status.pending,
                ScriptedFollowingStatus.next_due <= datetime.utcnow()
            )
        ).all()

        for p in due_pending:
            try:
                self._api.create_friendship(user_id=p.id)

                p.status = Status.following
                p.next_due = datetime.utcnow() + timedelta(days=14)
            except Exception as e:
                logger.error(f'Could not follow {p.id}: {e}')
                self._db.delete(p)
            finally:
                self._db.commit()

    def _validate_following(self):
        following_script = self._db.query(ScriptedFollowingStatus).filter(
            and_(
                ScriptedFollowingStatus.status.in_((Status.following, Status.mutual)),
                ScriptedFollowingStatus.next_due <= datetime.utcnow()
            )
        ).all()

        for f in following_script:
            if self._db.query(Follower).filter(Follower.id == f.id).first() is None:
                try:
                    self._api.destroy_friendship(user_id=f.id)

                    f.status = Status.expired
                    f.next_due = None
                except Exception as e:
                    logger.error(f'Could not unfollow {f.id}: {e}')
                    self._db.delete(f)
            else:
                f.status = Status.mutual
                f.next_due = datetime.utcnow() + self._following_followup_time

            self._db.commit()


def main(
        db_path: Optional[Path] = typer.Option(environ.get('FOLGMEG_DB_PATH', 'folgmeg')),
        consumer_key: str = typer.Option(environ.get('FOLGMEG_CONSUMER_KEY')),
        consumer_secret: str = typer.Option(environ.get('FOLGMEG_CONSUMER_SECRET')),
        access_token: str = typer.Option(environ.get('FOLGMEG_ACCESS_TOKEN')),
        access_token_secret: str = typer.Option(environ.get('FOLGMEG_ACCESS_TOKEN_SECRET')),
        target_inflight_ratio: float = typer.Option(float(environ.get('FOLGMEG_TARGET_INFLIGHT_RATIO', 0.10))),
        following_sample_size: int = typer.Option(int(environ.get('FOLGMEG_FOLLOWING_SAMPLE_SIZE', 10))),
        activity_num_days_lookback: int = typer.Option(int(environ.get('FOLGMEG_ACTIVITY_NUM_DAYS_LOOKBACK', 7))),
        tweet_sample_size: int = typer.Option(int(environ.get('FOLGMEG_TWEET_SAMPLE_SIZE', 100))),
        following_followup_num_days: int = typer.Option(int(environ.get('FOLGMEG_FOLLOWING_FOLLOWUP_NUM_DAYS', 14))),
):
    logging.basicConfig(
        stream=stdout,
        filemode="w",
        format=Log_Format,
        level=logging.INFO,
    )

    FolgMeg(
        db_path,
        consumer_key,
        consumer_secret,
        access_token,
        access_token_secret,
        target_inflight_ratio,
        following_sample_size,
        activity_num_days_lookback,
        tweet_sample_size,
        following_followup_num_days,
    ).run()


if __name__ == '__main__':
    typer.run(main)
