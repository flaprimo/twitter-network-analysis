from urllib.parse import quote
import logging

logger = logging.getLogger(__name__)


def query_builder(words, people=None, location=None, date=None, language=None):
    logger.info('building tw query')
    # for words, use:
    # "exact sentence" -> for exact sentences
    # #hashtag -> for hashtags
    # -word -> for words that must not be present
    q = f'{words}'

    if people:
        q += f' from:{people["from"]}' if 'from' in people else ''
        q += f' to:{people["to"]}' if 'to' in people else ''
        q += f' {people["mentions"]}' if 'mentions' in people else ''  # each mention needs "@" (eg: @user)

    if location:
        q += f' near:"{location["near"]}"' if 'near' in location else ''
        q += f' within:{location["within"]}mi' if 'within' in location else ''  # radius expressed in miles

    if date:
        q += f' since:{date["since"]}' if 'since' in date else ''  # start interval date (eg: 2018-11-06)
        q += f' until:{date["until"]}' if 'until' in date else ''  # end interval date (eg: 2018-11-12)

    q = f'q={quote(q, safe="")}'
    query = f'l={language}&{q}' if language else q
    logger.debug(f'built url query: {query}')

    return query
