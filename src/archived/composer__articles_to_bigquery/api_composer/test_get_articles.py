''' test get articles '''
import json
from .get_articles import get_articles, get_articles_to_dataframe, get_mapped_article_data
import unittest
from unittest.mock import MagicMock

# integration tests for api
# def test_get_articles():
#     '''test get articles'''
#     url = ('{apiurl}' +
#               '?direction=asc' +
#                 '&orderby=assetId' +
#                 '&limit={}' +
#                 '&q=(firstPublishTime%3Dge%3D2022-03-15T00%3A00%3A00%3B' +
#                 'firstPublishTime%3Dle%3D2022-03-15T23%3A59%3A59.999%3B' +
#                 'status%3Dmatch%3DREADY%3BlastActionPerformed%3Dmatch%3DPUBLISH)').format(1)

#     response = get_articles(url)
#     assert response.status_code == 200
#     assert response.json()['total'] > 0

# def test_get_articles_to_dataframe():
#     ''' test get articles to dataframe '''
#     url = ('{apiurl}' +
#               '?direction=asc' +
#                 '&orderby=assetId' +
#                 '&limit={}' +
#                 '&q=(firstPublishTime%3Dge%3D2022-03-15T00%3A00%3A00%3B' +
#                 'firstPublishTime%3Dle%3D2022-03-15T23%3A59%3A59.999%3B' +
#                 'status%3Dmatch%3DREADY%3BlastActionPerformed%3Dmatch%3DPUBLISH)').format(100)
#     response = get_articles_to_dataframe(url)
#     assert len(response) > 0
#     assert 'assetId' in response.columns
#     assert 'type' in response.columns
#     assert 'headline' in response.columns
#     assert 'href' in response.columns


class TestGetMappedArticleData(unittest.TestCase):
    ''' test get mapped article data '''

    def setUp(self):
        # Set up mocked data
        self.mock_response = MagicMock()
        self.mock_response.json.return_value = {
            'assetId': 12345,
            'headline': 'This is a headline',
            'byline': 'Author Name',
            'firstPublishTime': '2021-07-19T14:13:52Z',
            'sources': [{'source': 'Source 1'}],
            'brandsAndCategories': [{
                'brands': [{'brand': 'Brand 1'}],
                'categories': [{'category': 'Category 1/Category 2/Category 3'}]
            }],
            'currentWordCount': 400,
            'printSlug': '',
            'advertisement': False,
            'sponsored': False,
            'promotedBrand': False,
            'allowComments': True,
            'relatedAssets': [
                {'type': 'RelatedImage', 'url': 'http://example.com/image1.jpg'},
                {'type': 'RelatedVideo', 'url': 'http://example.com/video1.mp4'}
            ]
        }

    def test_content_id(self):
        ''' test content id '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['content_id'], 12345)

    def test_title(self):
        ''' test title '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['title'], 'This is a headline')

    def test_author(self):
        ''' test author '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['author'], 'Author Name')

    def test_publish_date(self):
        ''' test publish date '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['published_dts'], '2021-07-19T14:13:52Z')

    def test_source(self):
        ''' test source '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['source'], 'Source 1')

    def test_brand(self):
        ''' test brand '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['brand'], 'Brand 1')

    def test_category(self):
        ''' test category '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['category'], 'Category 1/Category 2/Category 3')

    def test_word_count(self):
        ''' test word count '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['word_count'], 400)

    def test_print_slug(self):
        ''' test print slug '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['print_slug'], '')

    def test_advertisement(self):
        ''' test advertisement '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['advertisement'], False)

    def test_sponsored(self):
        ''' test sponsored '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['sponsored'], False)

    def test_promoted_brand(self):
        ''' test promoted brand '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['promoted_flag'], False)

    def test_allow_comments(self):
        ''' test allow comments '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['comments_flag'], True)

    def test_related_image(self):
        ''' test related image '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['image_count'], 1)

    def test_related_video(self):
        ''' test related video '''
        mapped_data = get_mapped_article_data(self.mock_response, 12345 )
        self.assertEqual(mapped_data['video_count'], 1)
