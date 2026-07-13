from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import (
    classify_pmc_file,
    is_thumbnail_by_size,
    is_paired_thumbnail,
    is_inline_image,
    is_online_color,
    is_print_bw,
    has_color_twin,
    THUMBNAIL_MAX_SIZE_BYTES,
)


class TestIsThumbnailBySize:

    def test_thresholds_are_the_expected_values(self):
        # Lock the agreed SCRUM-6281 thresholds so they can't drift silently.
        assert THUMBNAIL_MAX_SIZE_BYTES == {'gif': 25000, 'jpg': 15000, 'jpeg': 15000}

    def test_none_size_is_not_a_thumbnail(self):
        assert is_thumbnail_by_size('gif', None) is False

    def test_gif_below_threshold_is_thumbnail(self):
        assert is_thumbnail_by_size('gif', 24999) is True

    def test_gif_at_or_above_threshold_is_not_thumbnail(self):
        assert is_thumbnail_by_size('gif', 25000) is False
        assert is_thumbnail_by_size('gif', 40000) is False

    def test_jpg_and_jpeg_share_the_lower_threshold(self):
        assert is_thumbnail_by_size('jpg', 14999) is True
        assert is_thumbnail_by_size('jpeg', 14999) is True
        assert is_thumbnail_by_size('jpg', 15000) is False
        assert is_thumbnail_by_size('jpeg', 20000) is False

    def test_extension_is_case_insensitive(self):
        assert is_thumbnail_by_size('GIF', 10000) is True

    def test_extensions_without_a_threshold_are_never_thumbnails(self):
        # tif/tiff/png are only ever name-classified, never size-classified.
        assert is_thumbnail_by_size('tif', 1000) is False
        assert is_thumbnail_by_size('png', 1000) is False


class TestClassifyPmcFile:

    def test_nxml_is_nxml(self):
        assert classify_pmc_file('main', 'nxml') == 'nXML'

    def test_non_image_is_supplement(self):
        assert classify_pmc_file('table1', 'xlsx') == 'supplement'
        assert classify_pmc_file('data', 'pdf') == 'supplement'

    def test_thumb_in_name_wins_regardless_of_size(self):
        # Publisher-labeled thumbnails (JoVE, Royal Society) stay thumbnails
        # even when large or when no size is supplied.
        assert classify_pmc_file('jove-76-50447-thumb', 'gif') == 'thumbnail'
        assert classify_pmc_file('rsob220308.thumb', 'jpg', 999999) == 'thumbnail'

    def test_image_without_size_and_without_thumb_is_figure(self):
        # Backward-compatible 2-arg call path (e.g. update_referencefile_class).
        assert classify_pmc_file('fig1', 'gif') == 'figure'
        assert classify_pmc_file('fig1', 'jpg') == 'figure'

    def test_small_gif_is_thumbnail_large_gif_is_figure(self):
        assert classify_pmc_file('5fig1', 'gif', 13224) == 'thumbnail'
        # A large gif (real full figure, e.g. BMC "*_HTML") stays a figure.
        assert classify_pmc_file('13072_2017_159_Fig5_HTML', 'gif', 555922) == 'figure'

    def test_small_jpg_is_thumbnail_large_jpg_is_figure(self):
        assert classify_pmc_file('somefig', 'jpg', 14000) == 'thumbnail'
        assert classify_pmc_file('5fig1', 'jpg', 107388) == 'figure'
        # 16 KB jpg is above the 15 KB jpg threshold -> figure.
        assert classify_pmc_file('somefig', 'jpg', 16000) == 'figure'

    def test_tif_is_not_size_classified(self):
        # tif has no size threshold, so even a tiny one is a figure.
        assert classify_pmc_file('imgtif', 'tif', 5000) == 'figure'

    def test_large_gif_with_larger_jpg_sibling_is_thumbnail(self):
        # SCRUM-6095: a gif above the 25 KB cutoff that is paired with a larger
        # same-named jpg master is still a thumbnail (real AGRKB:...211800 case:
        # 73 KB gif vs 340 KB jpg).
        assert classify_pmc_file('gr2', 'gif', 73784,
                                 sibling_sizes={'jpg': 340500}) == 'thumbnail'

    def test_large_gif_without_sibling_stays_figure(self):
        # No same-named jpg companion -> genuine standalone gif figure.
        assert classify_pmc_file('gr2', 'gif', 73784, sibling_sizes={}) == 'figure'
        assert classify_pmc_file('gr2', 'gif', 73784) == 'figure'

    def test_large_gif_bigger_than_jpg_sibling_stays_figure(self):
        # If the gif is the larger of the pair it is not the thumbnail.
        assert classify_pmc_file('gr2', 'gif', 400000,
                                 sibling_sizes={'jpg': 340500}) == 'figure'

    def test_large_jpg_with_gif_sibling_stays_figure(self):
        # Only gifs are reclassified via pairing; the jpg is the master.
        assert classify_pmc_file('gr2', 'jpg', 340500,
                                 sibling_sizes={'gif': 73784}) == 'figure'


class TestInlineImage:

    def test_ilm_suffix_is_inline_image(self):
        # Taylor & Francis inline images, classified before the size rule so
        # they don't fall through to thumbnail.
        assert classify_pmc_file('KRNB_A_2685379_ILM0001', 'jpg', 2577) == 'inline_image'
        assert classify_pmc_file('KRNB_A_2685379_ILM0002', 'jpg', 427) == 'inline_image'

    def test_ilm_is_case_insensitive(self):
        assert classify_pmc_file('foo_ilm12', 'jpg', 500) == 'inline_image'

    def test_author_names_containing_ilm_are_not_inline_images(self):
        # Guard against the '%ILM%' substring trap (Yilmaz, Egilmez, Tilmann).
        assert is_inline_image('00045679_Yilmaz14') is False
        assert is_inline_image('00001904_Egilmez94') is False
        assert is_inline_image('gr1_ILMitem') is False  # ILM not followed by digits/suffix

    def test_ilm_helper_matches_only_anchored_suffix(self):
        assert is_inline_image('x_ILM0001') is True
        assert is_inline_image('ILM0001_x') is False


class TestColorBwRenditions:

    # Real AGRKB:...301107 (KRNB / RNA Biology) names and sizes.
    NAMES = {
        'KRNB_A_2685379_F0001_OC', 'KRNB_A_2685379_F0001_PB',
        'KRNB_A_2685379_F0002_OC', 'KRNB_A_2685379_F0002_PB',
    }

    def test_oc_is_always_figure_even_when_small(self):
        # Rescue: a color figure under the size cutoff must not become thumbnail.
        assert classify_pmc_file('KRNB_A_2685379_F0001_OC', 'jpg', 5000) == 'figure'
        assert classify_pmc_file('KRNB_A_2685379_F0001_OC', 'jpg', 194701,
                                 sibling_display_names=self.NAMES) == 'figure'

    def test_pb_with_oc_twin_is_bw_duplicate(self):
        assert classify_pmc_file('KRNB_A_2685379_F0001_PB', 'jpg', 108779,
                                 sibling_display_names=self.NAMES) == 'bw_duplicate'

    def test_pb_without_oc_twin_stays_figure(self):
        # Sole rendition (no color twin present) -> figure, even if small.
        assert classify_pmc_file('SOME_F0001_PB', 'jpg', 5000,
                                 sibling_display_names={'SOME_F0002_PB'}) == 'figure'
        assert classify_pmc_file('SOME_F0001_PB', 'jpg', 108779) == 'figure'

    def test_helpers(self):
        assert is_online_color('x_F0001_OC') is True
        assert is_online_color('x_F0001_PB') is False
        assert is_print_bw('x_F0001_PB') is True
        assert is_print_bw('x_F0001_OC') is False
        assert has_color_twin('x_F0001_PB', {'x_F0001_OC', 'x_F0001_PB'}) is True
        assert has_color_twin('x_F0001_PB', {'x_F0002_OC'}) is False
        assert has_color_twin('x_F0001_PB', None) is False

    def test_twin_match_is_case_insensitive(self):
        assert has_color_twin('X_F1_PB', {'x_f1_oc'}) is True


class TestIsPairedThumbnail:

    def test_gif_smaller_than_jpg_sibling_is_thumbnail(self):
        assert is_paired_thumbnail('gif', 73784, {'jpg': 340500}) is True
        assert is_paired_thumbnail('gif', 10000, {'jpeg': 20000}) is True

    def test_gif_larger_than_jpg_sibling_is_not(self):
        assert is_paired_thumbnail('gif', 340500, {'jpg': 73784}) is False

    def test_no_sibling_is_not(self):
        assert is_paired_thumbnail('gif', 73784, None) is False
        assert is_paired_thumbnail('gif', 73784, {}) is False
        assert is_paired_thumbnail('gif', 73784, {'png': 999999}) is False

    def test_none_size_is_not(self):
        assert is_paired_thumbnail('gif', None, {'jpg': 340500}) is False
        assert is_paired_thumbnail('gif', 73784, {'jpg': None}) is False

    def test_non_gif_is_never_paired_thumbnail(self):
        assert is_paired_thumbnail('jpg', 73784, {'jpg': 340500}) is False
        assert is_paired_thumbnail('png', 10, {'jpg': 340500}) is False
