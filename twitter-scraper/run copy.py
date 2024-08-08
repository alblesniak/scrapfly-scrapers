from datetime import datetime

import pytz
from sqlalchemy import (JSON, Column, ForeignKey, Integer, String, Text,
                        create_engine)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

import twitter

Base = declarative_base()

# Definicja modelu TwitterProfile
class TwitterProfile(Base):
    __tablename__ = "twitter_profile"
    id = Column(String, primary_key=True)  # ID profilu
    rest_id = Column(String)
    name = Column(String)
    screen_name = Column(String)
    description = Column(Text)
    location = Column(String)
    followers_count = Column(Integer)
    friends_count = Column(Integer)
    statuses_count = Column(Integer)
    raw_json = Column(JSON)  # Cały JSON obiekt
    posts = relationship("TwitterPost", back_populates="profile")

# Definicja modelu TwitterPost
class TwitterPost(Base):
    __tablename__ = "twitter_posts"
    id = Column(String, primary_key=True)  # ID tweeta
    user_id = Column(String, ForeignKey("twitter_profile.id"))  # Klucz obcy do tabeli twitter_profile
    created_at = Column(String)
    text = Column(Text)
    favorite_count = Column(Integer)
    retweet_count = Column(Integer)
    reply_count = Column(Integer)
    quote_count = Column(Integer)
    raw_json = Column(JSON)  # Cały JSON obiekt
    profile = relationship("TwitterProfile", back_populates="posts")

# Ustawienie bazy danych SQLite
DATABASE_URL = "sqlite:///twitter_data.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Tworzenie tabel
Base.metadata.create_all(bind=engine)

def format_date_with_timezone(date_str, target_timezone="Europe/Warsaw"):
    """Konwertuje datę na format YYYY-MM-DD HH:MM w wybranej strefie czasowej."""
    parsed_date = datetime.strptime(date_str, '%a %b %d %H:%M:%S %z %Y')
    target_tz = pytz.timezone(target_timezone)
    local_date = parsed_date.astimezone(target_tz)
    return local_date.strftime('%Y-%m-%d %H:%M')

async def run():
    """Funkcja odpowiedzialna za scraping i zapisanie danych do bazy."""
    twitter.BASE_CONFIG["debug"] = True
    print("Running X.com tweet scrape and saving results to database")

    # Scrape profile tweets
    url = "https://twitter.com/donaldtusk"
    profile_tweets = await twitter.scrape_profile_tweets(url, limit=100)  # Limit to 100 tweets

    session = SessionLocal()

    try:
        profile_data = profile_tweets[0]["user"]
        
        # Zapis profilu do bazy danych
        profile = TwitterProfile(
            id=profile_data["id"],
            rest_id=profile_data["rest_id"],
            name=profile_data["name"],
            screen_name=profile_data["screen_name"],
            description=profile_data.get("description"),
            location=profile_data.get("location"),
            followers_count=profile_data.get("followers_count", 0),
            friends_count=profile_data.get("friends_count", 0),
            statuses_count=profile_data.get("statuses_count", 0),
            raw_json=profile_data
        )
        
        session.add(profile)
        session.commit()

        # Zapis tweetów do bazy danych
        for tweet in profile_tweets:
            post = TwitterPost(
                id=tweet["id"],
                user_id=profile.id,
                created_at=format_date_with_timezone(tweet['created_at'], "Europe/Warsaw"),
                text=tweet.get("text", ""),
                favorite_count=tweet.get("favorite_count", 0),
                retweet_count=tweet.get("retweet_count", 0),
                reply_count=tweet.get("reply_count", 0),
                quote_count=tweet.get("quote_count", 0),
                raw_json=tweet
            )
            
            session.add(post)
        
        session.commit()
        
    except IntegrityError:
        session.rollback()
        print("Data already exists in the database.")
    finally:
        session.close()

    print(f"Scraped and saved {len(profile_tweets)} tweets from the profile to the database.")

if __name__ == "__main__":
    import asyncio
    asyncio.run(run())
