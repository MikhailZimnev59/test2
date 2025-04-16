'''Программа, реализующая базовый функционал по поиску информации в таблице постов с категориями'''

from collections import Counter
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy import or_

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# settings - указать пользователя, пароль, порт PostgreSQL, имя базы данных, например:
# DATABASE_URL = "postgresql+asyncpg://user:password@localhost:5433/test_post"
DATABASE_URL = "postgresql+asyncpg://user:password@localhost/dbname"

Base = declarative_base()

class Post(Base):
    '''Класс постов с категоризацией'''
    __tablename__ = 'posts'

    id = Column(Integer, primary_key=True, index=True)
    category = Column(String, index=True)
    # когда я разрабатывал библиотечную систему, отдельным развлечением
    # был поиск по древовидным справочникам
    # например, предметный указатель

    content = Column(String)
    # можно добавить еще дату создания, дату коррекции, автора и многое другое

    def __repr__(self):
        return f"<Post(id={self.id}, category='{self.category}', content='{self.content[:20]}')>"


async def filter_and_process_posts(session: AsyncSession,
                                   category: str,
                                   keywords: list,
                                   limit: int,
                                   offset: int):
    '''Функция реализуюущая выборки по категории, ключевым словам из контента,
    размеру выборки и сдвигу, расчет количества слов и т.д.'''
    query = select(Post).where(
        Post.category == category,
        or_(*[Post.content.ilike(f'%{keyword}%') for keyword in keywords])
    ).offset(offset).limit(limit)

    result = await session.execute(query)
    posts = result.scalars().all()

    word_counter = Counter()
    tags = set()

    for post in posts:
        words = post.content.split()
        word_counter.update(words)

        tags.update(word for word in words if len(word) > 4)

    return {
        "posts": posts,
        "word_frequency": word_counter,
        "tags": list(tags),
        "total_count": len(posts)
    }

async def get_post(session: AsyncSession, post_id: int):
    '''Функция, выдающая пост по его id'''
    result = await session.execute(select(Post).where(Post.id == post_id))
    return result.scalar_one_or_none()


app = FastAPI()

engine = create_async_engine(DATABASE_URL, echo=True)
async_session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session
async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


@app.get("/posts/")
async def get_posts(
    category: str,
    keywords: str = '',  # Ключевые слова через запятую
    limit: int = 5,
    offset: int = 0,
    session: AsyncSession = Depends(get_session)
):
    '''Функция выдачи выборки по категории, по ключевым словам, по сдвигу и лимиту'''
    keywords_list = keywords.split(',')
    result = await filter_and_process_posts(session, category, keywords_list, limit, offset)
    return result


@app.get("/post/{post_id}")
async def read_post(post_id: int, db: AsyncSession = Depends(get_db)):
    '''Функция выдачи одного поста'''
    post = await get_post(db, post_id)
    if post is None:
        raise HTTPException(status_code=404, detail="Post not found")
    return post
