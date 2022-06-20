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

from models import Base, ScriptedFollowingStatus, Status, Follower

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
            num_followup_days,
            desc_exclusions,
    ):
        self._target_inflight_ratio = target_inflight_ratio
        self._followup_time = timedelta(days=num_followup_days)
        self._description_exclusions = desc_exclusions.split(",") if desc_exclusions else desc_exclusions

        self._db = self._init_db(db_path)

        self._api = tweepy.API(
            tweepy.OAuth1UserHandler(consumer_key, consumer_secret, access_token, access_token_secret),
            wait_on_rate_limit=False
        )

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
        self._identify_candidates()
        self._follow_candidates()
        self._validate_following()

    def _verify_self(self):
        try:
            self._me = self._api.verify_credentials(skip_status=True)
        except tweepy.errors.TweepyException as e:
            logger.error(f'Could not verify self: {e}')

    def _update_follower_list(self):
        self._verify_self()

        if self._db.query(Follower).count() == self._me.followers_count:
            return

        try:
            self._db.query(Follower).delete()
            self._db.bulk_save_objects(
                [
                    Follower(id=follower)
                    for follower in tweepy.Cursor(self._api.get_follower_ids, user_id=self._me.id).items()
                ]
            )

            self._db.commit()
        except tweepy.TweepyException as e:
            self._db.rollback()

            logger.error(f'Could not update followers of self: {e}')

    def _identify_candidates(self):
        pending = self._db.query(ScriptedFollowingStatus).filter(
            ScriptedFollowingStatus.status == Status.pending
        ).count()

        target_inflight = int(self._db.query(Follower).count() * self._target_inflight_ratio)

        if pending >= target_inflight:
            return

        random_follower = self._db.query(Follower).order_by(func.random()).first()

        second_degree_followers = []
        try:
            second_degree_followers = self._api.get_follower_ids(user_id=random_follower.id)
        except tweepy.TweepyException as e:
            logger.error(f'Could not fetch followers of {random_follower.id}: {e}')

        second_degree_followers = random.sample(second_degree_followers, target_inflight - pending)
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        for follower in second_degree_followers:
            # Skip if they are already on our radar
            if self._db.query(ScriptedFollowingStatus).filter(ScriptedFollowingStatus.id == follower).count() == 1:
                continue

            # Skip if they are already a follower
            if self._db.query(Follower).filter(Follower.id == follower).count() == 1:
                continue

            if self._description_exclusions:
                try:
                    f = self._api.get_user(user_id=follower)

                    if any([exclude in f.description for exclude in self._description_exclusions]):
                        logger.info(f'User description contains an exclusion phrase, skipping: {follower}')

                        continue

                except tweepy.TweepyException as e:
                    logger.error(f'Could not fetch user profile {follower} for exclusion phrase filtration: {e}')

            last_tweets = []
            try:
                last_tweets = [
                    t for t in self._api.user_timeline(
                        user_id=follower,
                        count=100,
                        include_rts=True,
                    )
                    if t.created_at.timestamp() > seven_days_ago.timestamp()
                ]
            except Exception as e:
                logger.error(f'Could not retrieve tweets for {follower}: {e}')

            # Skip if they're not active
            if len(last_tweets) == 0:
                continue

            hours = {h: 0 for h in range(0, 24)}
            for tweet in last_tweets:
                hour = tweet.created_at.hour
                hours[hour] = hours[hour] + 1

            earliest_most_common_hour = max(hours, key=hours.get)

            tomorrow = datetime.utcnow() + timedelta(days=1)
            follow_time = tomorrow.replace(hour=earliest_most_common_hour)

            self._db.add(
                ScriptedFollowingStatus(
                    id=follower,
                    status=Status.pending,
                    next_due=follow_time
                )
            )
            self._db.commit()

            logger.info(f'Added candidate {follower}, to be followed after {follow_time}')

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
                p.next_due = datetime.utcnow() + self._followup_time

                logger.info(f'Followed {p.id}, to check for followback after {p.next_due}')
            except Exception as e:
                self._db.delete(p)

                logger.error(f'Could not follow {p.id}: {e}')
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

                    logger.info(f'Unfollowed {f.id}')
                except Exception as e:
                    self._db.delete(f)
                    logger.error(f'Could not unfollow {f.id}: {e}')
            else:
                f.status = Status.mutual
                f.next_due = datetime.utcnow() + self._followup_time

                logger.info(f'Mutual follower {f.id}, due for checkup after {f.next_due}')

            self._db.commit()


def main(
        db_path: Optional[Path] = typer.Option(environ.get('FOLGMEG_DB_PATH', 'folgmeg')),
        consumer_key: str = typer.Option(environ.get('FOLGMEG_CONSUMER_KEY')),
        consumer_secret: str = typer.Option(environ.get('FOLGMEG_CONSUMER_SECRET')),
        access_token: str = typer.Option(environ.get('FOLGMEG_ACCESS_TOKEN')),
        access_token_secret: str = typer.Option(environ.get('FOLGMEG_ACCESS_TOKEN_SECRET')),
        target_inflight_ratio: float = typer.Option(float(environ.get('FOLGMEG_TARGET_INFLIGHT_RATIO', 0.02))),
        num_followup_days: int = typer.Option(int(environ.get('FOLGMEG_NUM_FOLLOWUP_DAYS', 3))),
        description_exclusions: str = typer.Option(
            environ.get('FOLGMEG_DESCRIPTION_EXCLUSIONS'),
            help="Comma separated string, e.g. abc,123,def"
        ),
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
        num_followup_days,
        description_exclusions,
    ).run()


if __name__ == '__main__':
    typer.run(main)
