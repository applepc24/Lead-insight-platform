"""도메인별 공고 파서의 현재 동작을 고정하는 특성 테스트.

파서 리팩터링(JSON-LD 추출 공통화, 도메인 설정 일원화)이 동작을 바꾸지 않았는지
확인하기 위한 안전망이다. 따라서 "이래야 한다"가 아니라 "현재 이렇다"를 기록한다.
사이트별 비대칭(catch만 배열, jobkorea만 locality 우선 등)도 의도적으로 고정한다.
"""

import importlib
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import pytest

MODULE_UNDER_TEST = "worker_job_postings_to_s3"

worker = importlib.import_module(MODULE_UNDER_TEST)


CONTRACT_FIELDS = {
    "company_name",
    "title",
    "location",
    "employment_type",
    "experience_level",
    "description_text",
}


def build_html(
    *,
    title=None,
    og_title=None,
    meta_description=None,
    json_ld=None,
    canonical=None,
    body="",
) -> str:
    """파서가 실제로 읽는 요소만 담은 최소 HTML 픽스처."""
    head = []
    if title is not None:
        head.append(f"<title>{title}</title>")
    if og_title is not None:
        head.append(f'<meta property="og:title" content="{og_title}">')
    if meta_description is not None:
        head.append(f'<meta name="description" content="{meta_description}">')
    if canonical is not None:
        head.append(f'<link rel="canonical" href="{canonical}">')
    if json_ld is not None:
        payload = json_ld if isinstance(json_ld, str) else json.dumps(json_ld, ensure_ascii=False)
        head.append(f'<script type="application/ld+json">{payload}</script>')

    return f"<html><head>{''.join(head)}</head><body>{body}</body></html>"


def make_jobposting_ld(
    *,
    title="백엔드 개발자",
    company="테스트컴퍼니",
    region="서울",
    locality="강남구",
    employment_type="FULL_TIME",
    experience="경력 3년 이상",
    description="<p>주요 업무</p><br>지원 자격",
    location_as_list=False,
) -> dict:
    address = {}
    if region is not None:
        address["addressRegion"] = region
    if locality is not None:
        address["addressLocality"] = locality

    job_location = {"@type": "Place", "address": address}

    ld = {
        "@context": "https://schema.org",
        "@type": "JobPosting",
        "title": title,
        "hiringOrganization": {"@type": "Organization", "name": company},
        "jobLocation": [job_location] if location_as_list else job_location,
    }
    if employment_type is not None:
        ld["employmentType"] = employment_type
    if experience is not None:
        ld["experienceRequirements"] = experience
    if description is not None:
        ld["description"] = description
    return ld


ALL_PARSERS = [
    worker.extract_wanted_fields,
    worker.extract_groupby_fields,
    worker.extract_saramin_fields,
    worker.extract_catch_fields,
    worker.extract_jobkorea_fields,
]


class TestParserContract:
    """모든 파서는 출처와 무관하게 동일한 6개 필드를 반환한다.

    하류(curated → stg_job_postings)가 사이트를 몰라도 되게 하는 핵심 계약이다.
    """

    @pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
    def test_parser_should_return_exactly_six_contract_fields(self, parser):
        html = build_html(
            title="[테스트컴퍼니] 백엔드 개발자 채용",
            og_title="[테스트컴퍼니] 백엔드 개발자",
            meta_description="테스트컴퍼니, 백엔드 개발자, 경력: 3년",
            json_ld=make_jobposting_ld(),
        )

        result = parser(html)

        assert set(result.keys()) == CONTRACT_FIELDS

    @pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
    def test_parser_should_return_all_none_for_empty_html(self, parser):
        """메타데이터가 하나도 없어도 예외 없이 6필드를 채워 반환해야 한다."""
        result = parser("<html><head></head><body></body></html>")

        assert set(result.keys()) == CONTRACT_FIELDS
        assert all(value is None for value in result.values())

    @pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
    def test_parser_should_not_raise_on_malformed_html(self, parser):
        """외부 사이트 HTML은 통제 밖이라 깨진 입력에도 DLQ로 가지 않아야 한다."""
        result = parser("<html><head><title>닫히지 않은")

        assert set(result.keys()) == CONTRACT_FIELDS

    @pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
    def test_parser_should_not_raise_when_json_ld_fields_are_null(self, parser):
        """JSON-LD 필드가 전부 null이어도 타입 방어(or {} / isinstance)가 동작한다."""
        html = build_html(
            json_ld={
                "@type": "JobPosting",
                "title": None,
                "hiringOrganization": None,
                "jobLocation": None,
                "employmentType": None,
                "experienceRequirements": None,
                "description": None,
            }
        )

        result = parser(html)

        assert set(result.keys()) == CONTRACT_FIELDS

    @pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
    def test_parser_should_not_raise_when_json_ld_fields_have_wrong_types(self, parser):
        """hiringOrganization이 dict가 아닌 문자열로 오는 등 스키마 위반에도 견딘다."""
        html = build_html(
            json_ld={
                "@type": "JobPosting",
                "title": "백엔드 개발자",
                "hiringOrganization": "테스트컴퍼니",
                "jobLocation": "서울",
            }
        )

        result = parser(html)

        assert set(result.keys()) == CONTRACT_FIELDS


class TestJsonLdExtraction:
    def test_should_extract_jobposting_from_single_object(self):
        html = build_html(json_ld=make_jobposting_ld(title="데이터 엔지니어"))

        result = worker.extract_jobposting_json_ld(html)

        assert result is not None
        assert result["title"] == "데이터 엔지니어"

    def test_should_extract_jobposting_from_array_payload(self):
        """ld+json 하나에 배열로 여러 객체가 들어오는 실제 사이트 패턴."""
        payload = [
            {"@type": "BreadcrumbList", "itemListElement": []},
            make_jobposting_ld(title="플랫폼 엔지니어"),
        ]
        html = build_html(json_ld=payload)

        result = worker.extract_jobposting_json_ld(html)

        assert result is not None
        assert result["title"] == "플랫폼 엔지니어"

    def test_should_return_none_when_no_jobposting_type(self):
        html = build_html(json_ld={"@type": "Organization", "name": "테스트컴퍼니"})

        assert worker.extract_jobposting_json_ld(html) is None

    def test_should_skip_invalid_json_and_keep_following_block(self):
        """깨진 ld+json 블록이 있어도 뒤따르는 정상 블록은 살려야 한다.

        외부 HTML은 통제 밖이므로 한 조각의 실패가 job 전체를 DLQ로 보내면 안 된다.
        """
        broken = '<script type="application/ld+json">{not valid json,,}</script>'
        valid = (
            '<script type="application/ld+json">'
            + json.dumps(make_jobposting_ld(title="살아남은 공고"), ensure_ascii=False)
            + "</script>"
        )
        html = f"<html><head>{broken}{valid}</head><body></body></html>"

        result = worker.extract_jobposting_json_ld(html)

        assert result is not None
        assert result["title"] == "살아남은 공고"

    def test_should_skip_empty_script_block(self):
        html = '<html><head><script type="application/ld+json"></script></head></html>'

        assert worker.extract_json_ld_objects(html) == []

    def test_should_return_none_when_no_json_ld_at_all(self):
        assert worker.extract_jobposting_json_ld(build_html(title="JSON-LD 없는 페이지")) is None

    def test_should_not_match_when_type_is_a_list(self):
        """알려진 한계: @type을 == 로 비교해 배열 표기를 놓친다.

        schema.org에서 ["JobPosting", "Thing"] 은 유효한 표현이다.
        """
        ld = make_jobposting_ld()
        ld["@type"] = ["JobPosting", "Thing"]

        assert worker.extract_jobposting_json_ld(build_html(json_ld=ld)) is None


class TestJoinLdValues:
    """JSON-LD 필드가 문자열/배열 양쪽으로 오는 것을 흡수하는 공통 헬퍼."""

    def test_should_return_string_value_as_is(self):
        assert worker.join_ld_values("FULL_TIME") == "FULL_TIME"

    def test_should_join_list_with_comma(self):
        assert worker.join_ld_values(["FULL_TIME", "CONTRACT"]) == "FULL_TIME, CONTRACT"

    def test_should_drop_falsy_items_from_list(self):
        assert worker.join_ld_values(["FULL_TIME", None, "", 0]) == "FULL_TIME"

    def test_should_stringify_non_string_scalar(self):
        assert worker.join_ld_values(3) == "3"

    @pytest.mark.parametrize("value", [None, "", 0])
    def test_should_return_none_for_falsy_scalar(self, value):
        assert worker.join_ld_values(value) is None

    def test_should_return_empty_string_for_empty_list(self):
        """빈 배열은 리팩터링 이전과 동일하게 빈 문자열을 낸다 (None 이 아님)."""
        assert worker.join_ld_values([]) == ""


class TestExtractLdLocation:
    """jobLocation 이 dict / 배열 어느 쪽으로 와도 같은 결과를 내야 한다."""

    @pytest.mark.parametrize(
        "region, locality, expected",
        [
            ("서울", "강남구", "서울 강남구"),
            ("서울", "서울", "서울"),
            ("부산", None, "부산"),
            (None, "판교", "판교"),
            (None, None, None),
        ],
    )
    def test_should_resolve_region_and_locality(self, region, locality, expected):
        address = {}
        if region is not None:
            address["addressRegion"] = region
        if locality is not None:
            address["addressLocality"] = locality

        assert worker.extract_ld_location({"address": address}) == expected

    def test_should_use_first_entry_when_given_a_list(self):
        job_location = [
            {"address": {"addressRegion": "서울", "addressLocality": "강남구"}},
            {"address": {"addressRegion": "부산", "addressLocality": "해운대구"}},
        ]

        assert worker.extract_ld_location(job_location) == "서울 강남구"

    def test_dict_and_single_item_list_should_agree(self):
        """같은 주소면 표현 형태와 무관하게 같은 값이 나와야 한다."""
        place = {"address": {"addressRegion": "서울", "addressLocality": "강남구"}}

        assert worker.extract_ld_location(place) == worker.extract_ld_location([place])

    @pytest.mark.parametrize(
        "job_location",
        [None, [], "서울", 123, {"address": "서울"}, {"address": None}, [None], ["서울"]],
    )
    def test_should_return_none_for_unusable_shapes(self, job_location):
        assert worker.extract_ld_location(job_location) is None


class TestExtractFromJobpostingLd:
    """공통 추출기는 값만 만들어 돌려주고, 무엇을 쓸지는 각 파서가 고른다."""

    def test_should_return_expected_keys(self):
        result = worker.extract_from_jobposting_ld(make_jobposting_ld())

        assert set(result.keys()) == {
            "company_name",
            "title",
            "location",
            "employment_type",
            "experience_level",
            "description",
        }

    def test_should_extract_all_values(self):
        result = worker.extract_from_jobposting_ld(make_jobposting_ld())

        assert result["company_name"] == "테스트컴퍼니"
        assert result["title"] == "백엔드 개발자"
        assert result["location"] == "서울 강남구"
        assert result["employment_type"] == "FULL_TIME"
        assert result["experience_level"] == "경력 3년 이상"

    def test_should_return_description_unprocessed(self):
        """정제 방식이 사이트마다 달라 원문 그대로 넘긴다 (태그 제거하지 않음)."""
        ld = make_jobposting_ld(description="<p>주요 업무</p>")

        assert worker.extract_from_jobposting_ld(ld)["description"] == "<p>주요 업무</p>"

    def test_should_return_none_values_for_empty_jobposting(self):
        result = worker.extract_from_jobposting_ld({})

        assert all(value is None for value in result.values())

    @pytest.mark.parametrize("org", [None, "테스트컴퍼니", 123, []])
    def test_should_survive_non_dict_hiring_organization(self, org):
        result = worker.extract_from_jobposting_ld({"hiringOrganization": org})

        assert result["company_name"] is None


class TestWantedParser:
    def test_should_prefer_json_ld_over_title_tag(self):
        html = build_html(
            title="[무시될회사] 무시될제목 | 원티드",
            json_ld=make_jobposting_ld(title="백엔드 개발자", company="원티드컴퍼니"),
        )

        result = worker.extract_wanted_fields(html)

        assert result["company_name"] == "원티드컴퍼니"
        assert result["title"] == "백엔드 개발자"

    def test_should_join_region_and_locality_when_different(self):
        html = build_html(json_ld=make_jobposting_ld(region="서울", locality="강남구"))

        assert worker.extract_wanted_fields(html)["location"] == "서울 강남구"

    def test_should_not_duplicate_when_region_equals_locality(self):
        html = build_html(json_ld=make_jobposting_ld(region="서울", locality="서울"))

        assert worker.extract_wanted_fields(html)["location"] == "서울"

    def test_should_use_region_when_locality_missing(self):
        html = build_html(json_ld=make_jobposting_ld(region="부산", locality=None))

        assert worker.extract_wanted_fields(html)["location"] == "부산"

    def test_should_use_locality_when_region_missing(self):
        html = build_html(json_ld=make_jobposting_ld(region=None, locality="판교"))

        assert worker.extract_wanted_fields(html)["location"] == "판교"

    def test_should_also_read_job_location_from_list(self):
        """B 리팩터링에서 넓어진 동작 — 이전에는 dict만 처리했다."""
        html = build_html(
            json_ld=make_jobposting_ld(region="서울", locality="강남구", location_as_list=True)
        )

        assert worker.extract_wanted_fields(html)["location"] == "서울 강남구"

    def test_should_join_employment_type_list_with_comma(self):
        html = build_html(json_ld=make_jobposting_ld(employment_type=["FULL_TIME", "CONTRACT"]))

        assert worker.extract_wanted_fields(html)["employment_type"] == "FULL_TIME, CONTRACT"

    def test_should_drop_falsy_items_when_joining_employment_type(self):
        html = build_html(json_ld=make_jobposting_ld(employment_type=["FULL_TIME", None, ""]))

        assert worker.extract_wanted_fields(html)["employment_type"] == "FULL_TIME"

    def test_should_join_experience_list_with_comma(self):
        html = build_html(json_ld=make_jobposting_ld(experience=["신입", "경력"]))

        assert worker.extract_wanted_fields(html)["experience_level"] == "신입, 경력"

    def test_should_strip_html_tags_from_json_ld_description(self):
        """wanted의 description은 HTML 조각이라 태그를 제거한다."""
        html = build_html(
            json_ld=make_jobposting_ld(description="<p>주요 업무</p><br>지원 자격 &amp; 우대사항")
        )

        description = worker.extract_wanted_fields(html)["description_text"]

        assert "<p>" not in description
        assert "주요 업무" in description
        assert "지원 자격 & 우대사항" in description

    def test_should_fall_back_to_title_tag_when_json_ld_missing(self):
        """폴백 체인: JSON-LD 없음 → <title>에서 [회사] 제목 분리."""
        html = build_html(
            title="[원티드컴퍼니] 데이터 엔지니어 채용 공고 | 원티드",
            meta_description="원티드컴퍼니에서 데이터 엔지니어를 채용합니다",
        )

        result = worker.extract_wanted_fields(html)

        assert result["company_name"] == "원티드컴퍼니"
        assert result["title"] == "데이터 엔지니어"
        assert result["description_text"] == "원티드컴퍼니에서 데이터 엔지니어를 채용합니다"

    def test_should_keep_json_ld_title_while_filling_company_from_title_tag(self):
        """JSON-LD가 채운 값은 폴백이 덮어쓰지 않는다 (`기존값 or 폴백` 패턴).

        회사가 비어 폴백 블록이 돌더라도 title은 JSON-LD 값을 지켜야 한다.
        """
        ld = make_jobposting_ld(title="시니어 백엔드 개발자", company=None)
        html = build_html(title="[폴백컴퍼니] 다른 제목 | 원티드", json_ld=ld)

        result = worker.extract_wanted_fields(html)

        assert result["company_name"] == "폴백컴퍼니"
        assert result["title"] == "시니어 백엔드 개발자"

    def test_should_keep_json_ld_company_while_filling_title_from_title_tag(self):
        """위 테스트의 대칭 케이스 — title이 비어 폴백이 돌아도 company는 지켜야 한다.

        `or` 를 빼면 폴백의 회사명이 JSON-LD 값을 덮어쓴다. B 리팩터링에서
        폴백 로직을 옮길 때 가장 놓치기 쉬운 회귀라 양방향 모두 고정한다.
        """
        ld = make_jobposting_ld(title=None, company="원티드컴퍼니")
        html = build_html(title="[폴백컴퍼니] 폴백 제목 | 원티드", json_ld=ld)

        result = worker.extract_wanted_fields(html)

        assert result["company_name"] == "원티드컴퍼니"
        assert result["title"] == "폴백 제목"

    def test_should_ignore_og_title_and_use_title_tag_only(self):
        """wanted 파서는 다른 파서와 달리 og:title을 보지 않는다."""
        html = build_html(title="[타이틀컴퍼니] 타이틀 제목 | 원티드", og_title="[og컴퍼니] og 제목")

        result = worker.extract_wanted_fields(html)

        assert result["company_name"] == "타이틀컴퍼니"


class TestGroupbyParser:
    def test_should_extract_fields_from_json_ld(self):
        html = build_html(json_ld=make_jobposting_ld(title="프론트엔드 개발자", company="그룹바이"))

        result = worker.extract_groupby_fields(html)

        assert result["company_name"] == "그룹바이"
        assert result["title"] == "프론트엔드 개발자"

    def test_should_extract_experience_from_mugwan_prefix(self):
        """title 태그가 '무관 '으로 시작하면 경력을 '무관'으로 본다."""
        html = build_html(title="무관 백엔드 개발자 채용")

        assert worker.extract_groupby_fields(html)["experience_level"] == "무관"

    def test_should_strip_mugwan_prefix_from_title(self):
        html = build_html(title="무관 백엔드 개발자")

        assert worker.extract_groupby_fields(html)["title"] == "백엔드 개발자"

    def test_should_remove_mugwan_anywhere_in_title(self):
        """알려진 한계: replace가 전역 치환이라 제목 중간의 '무관 '도 지워진다.

        접두사만 제거하려는 의도였다면 removeprefix가 맞다. 현재 동작을 고정해둔다.
        """
        html = build_html(title="백엔드 무관 개발자")

        assert worker.extract_groupby_fields(html)["title"] == "백엔드 개발자"

    @pytest.mark.parametrize(
        "title_text, expected",
        [
            ("백엔드 개발자 (신입)", "신입"),
            ("백엔드 개발자 (3년 이하)", "3년 이하"),
            ("백엔드 개발자 (경력무관)", "경력무관"),
        ],
    )
    def test_should_extract_experience_from_parenthesis(self, title_text, expected):
        html = build_html(title=title_text)

        assert worker.extract_groupby_fields(html)["experience_level"] == expected

    def test_should_leave_experience_none_when_no_pattern_matches(self):
        html = build_html(title="백엔드 개발자 채용합니다")

        assert worker.extract_groupby_fields(html)["experience_level"] is None

    def test_should_prefer_og_title_over_title_tag(self):
        html = build_html(title="타이틀 태그 제목", og_title="og 제목")

        assert worker.extract_groupby_fields(html)["title"] == "og 제목"

    def test_should_not_read_experience_requirements_from_json_ld(self):
        """groupby는 JSON-LD의 experienceRequirements를 읽지 않는다.

        경력은 title 태그에서만 뽑는다. B 리팩터링에서 공통 함수로 묶다가
        실수로 동작이 바뀌기 가장 쉬운 지점이다.
        """
        html = build_html(json_ld=make_jobposting_ld(experience="경력 5년 이상"))

        assert worker.extract_groupby_fields(html)["experience_level"] is None

    def test_should_read_job_location_from_dict(self):
        html = build_html(json_ld=make_jobposting_ld(region="서울", locality="강남구"))

        assert worker.extract_groupby_fields(html)["location"] == "서울 강남구"


class TestSaraminParser:
    """사람인은 JSON-LD가 아예 없어 meta description 파싱에 전적으로 의존한다."""

    META_DESC = "사람인컴퍼니, 백엔드 개발자, 경력: 3년 이상, 서울"

    def test_should_parse_company_and_title_from_meta_description(self):
        result = worker.extract_saramin_fields(build_html(meta_description=self.META_DESC))

        assert result["company_name"] == "사람인컴퍼니"
        assert result["title"] == "백엔드 개발자"

    def test_should_extract_experience_from_meta_description(self):
        result = worker.extract_saramin_fields(build_html(meta_description=self.META_DESC))

        assert result["experience_level"] == "3년 이상"

    def test_should_keep_full_meta_description_as_description_text(self):
        """다른 사이트는 본문을 넣지만 사람인은 meta description 한 줄이 전부다.

        사람인의 description 품질 지표가 구조적으로 낮은 이유 — 버그가 아니라 소스의 한계.
        """
        result = worker.extract_saramin_fields(build_html(meta_description=self.META_DESC))

        assert result["description_text"] == self.META_DESC

    def test_should_prefer_bracket_company_from_og_title(self):
        html = build_html(
            og_title="[사람인컴퍼니] 백엔드 개발자 - 사람인",
            meta_description="다른회사, 다른제목",
        )

        result = worker.extract_saramin_fields(html)

        assert result["company_name"] == "사람인컴퍼니"
        assert result["title"] == "백엔드 개발자"

    def test_should_strip_deadline_and_site_suffix_from_title(self):
        html = build_html(og_title="[사람인컴퍼니] 백엔드 개발자(D-7) - 사람인")

        assert worker.extract_saramin_fields(html)["title"] == "백엔드 개발자"

    def test_suffix_stripping_is_order_dependent(self):
        """알려진 한계: '- 사람인' 을 먼저 지우므로 D-day가 뒤에 오면 둘 다 남는다.

        clean_saramin_title 이 접미사를 고정 순서로 벗기기 때문이다.
        """
        html = build_html(og_title="[사람인컴퍼니] 백엔드 개발자 - 사람인(D-7)")

        assert worker.extract_saramin_fields(html)["title"] == "백엔드 개발자 - 사람인"

    def test_should_ignore_json_ld_even_when_present(self):
        """사람인 파서는 JSON-LD를 읽지 않는다 — 실제 페이지에 없기 때문."""
        html = build_html(
            og_title="[사람인컴퍼니] 실제제목 - 사람인",
            json_ld=make_jobposting_ld(title="무시될제목", company="무시될회사"),
        )

        result = worker.extract_saramin_fields(html)

        assert result["title"] == "실제제목"
        assert result["company_name"] == "사람인컴퍼니"

    def test_should_stay_empty_when_only_json_ld_is_available(self):
        """meta 태그가 하나도 없고 JSON-LD만 있으면 아무것도 못 뽑는다.

        위 테스트만으로는 부족하다 — 사람인은 title을 `or` 없이 직접 대입하므로
        og:title 이 있으면 JSON-LD를 읽든 말든 결과가 같아져 회귀가 가려진다.
        JSON-LD 단독 케이스라야 "안 읽는다"가 실제로 검증된다.
        """
        html = build_html(json_ld=make_jobposting_ld(title="무시될제목", company="무시될회사"))

        result = worker.extract_saramin_fields(html)

        assert all(value is None for value in result.values())

    def test_should_always_leave_location_and_employment_type_none(self):
        """meta description에 지역이 있어도 별도 필드로 뽑지 않는 것이 현재 동작."""
        result = worker.extract_saramin_fields(build_html(meta_description=self.META_DESC))

        assert result["location"] is None
        assert result["employment_type"] is None

    def test_comma_in_company_name_shifts_parsed_fields(self):
        """알려진 한계: 콤마 분리라 회사명에 콤마가 있으면 제목이 밀린다."""
        html = build_html(meta_description="사람인, 주식회사, 백엔드 개발자")

        result = worker.extract_saramin_fields(html)

        assert result["company_name"] == "사람인"
        assert result["title"] == "주식회사"


class TestCatchParser:
    def test_should_read_job_location_from_list(self):
        """catch만 jobLocation이 배열이다 — 다른 사이트는 dict."""
        html = build_html(
            json_ld=make_jobposting_ld(region="서울", locality="종로구", location_as_list=True)
        )

        assert worker.extract_catch_fields(html)["location"] == "서울 종로구"

    def test_should_also_read_job_location_from_dict(self):
        """공통 함수가 dict/배열을 모두 흡수하므로 dict로 와도 읽는다.

        B 리팩터링에서 의도적으로 바꾼 동작이다. 이전에는 배열만 처리해
        dict로 오면 조용히 None 이 됐다. None 이 값으로 바뀌는 방향뿐이라
        기존에 맞던 값이 틀려질 여지는 없다.
        """
        html = build_html(json_ld=make_jobposting_ld(region="서울", locality="강남구"))

        assert worker.extract_catch_fields(html)["location"] == "서울 강남구"

    def test_should_parse_pipe_separated_meta_description_by_index(self):
        """캐치 meta description은 '|' 구분자의 위치로 필드를 결정한다."""
        html = build_html(meta_description="신입 | 정규직 | 대졸 | 초봉4000 | 서울 강남구 | 마감임박")

        result = worker.extract_catch_fields(html)

        assert result["experience_level"] == "신입"
        assert result["employment_type"] == "정규직"
        assert result["location"] == "서울 강남구"

    def test_should_skip_location_when_pipe_parts_too_few(self):
        """location은 5번째 조각이라 조각이 부족하면 채우지 않는다 (IndexError 방지)."""
        html = build_html(meta_description="신입 | 정규직 | 대졸")

        result = worker.extract_catch_fields(html)

        assert result["experience_level"] == "신입"
        assert result["employment_type"] == "정규직"
        assert result["location"] is None

    def test_pipe_index_is_positional_not_semantic(self):
        """알려진 위험: 인덱스 하드코딩이라 사이트가 순서를 바꾸면 조용히 틀린 값이 들어간다.

        예외도 로그도 없이 마트까지 흘러가므로 품질 지표로 관측해야 하는 지점이다.
        """
        html = build_html(meta_description="A | B | C | D | E")

        result = worker.extract_catch_fields(html)

        assert result["experience_level"] == "A"
        assert result["employment_type"] == "B"
        assert result["location"] == "E"

    def test_should_not_override_json_ld_values_with_meta_description(self):
        html = build_html(
            json_ld=make_jobposting_ld(employment_type="FULL_TIME", experience="경력 3년"),
            meta_description="신입 | 계약직 | 대졸 | 초봉4000 | 서울",
        )

        result = worker.extract_catch_fields(html)

        assert result["employment_type"] == "FULL_TIME"
        assert result["experience_level"] == "경력 3년"

    def test_should_extract_company_from_bracket_title(self):
        html = build_html(og_title="[캐치컴퍼니] 백엔드 개발자 채용 | 캐치")

        result = worker.extract_catch_fields(html)

        assert result["company_name"] == "캐치컴퍼니"
        assert result["title"] == "백엔드 개발자"

    def test_should_strip_deadline_parenthesis_from_title(self):
        html = build_html(og_title="[캐치컴퍼니] 백엔드 개발자 (~12/31) | 캐치")

        assert worker.extract_catch_fields(html)["title"] == "백엔드 개발자"

    def test_should_not_strip_html_tags_from_json_ld_description(self):
        """catch는 wanted와 달리 description에 strip_html_tags를 쓰지 않는다.

        공백 정규화만 하므로 태그가 그대로 남는다. B에서 통일하고 싶어질 수 있지만
        그건 동작 변경이므로 의도적으로 결정해야 한다.
        """
        html = build_html(json_ld=make_jobposting_ld(description="<p>주요 업무</p>"))

        assert worker.extract_catch_fields(html)["description_text"] == "<p>주요 업무</p>"


class TestJobkoreaParser:
    def test_should_split_company_and_title_by_bracket_pattern(self):
        """잡코리아 제목은 '회사명 [태그]제목' 형태라 정규식으로 분리한다."""
        html = build_html(
            og_title="잡코리아컴퍼니 채용 - 잡코리아컴퍼니 [경력] 백엔드 개발자 | 잡코리아"
        )

        result = worker.extract_jobkorea_fields(html)

        assert result["company_name"] == "잡코리아컴퍼니"
        assert result["title"] == "[경력] 백엔드 개발자"

    def test_should_leave_company_none_when_bracket_absent(self):
        """알려진 한계: 대괄호가 없으면 회사/제목 분리에 실패해 회사가 비게 된다.

        mart_job_postings_source_quality 의 missing_company_count 가 세는 대상.
        """
        html = build_html(og_title="잡코리아컴퍼니 채용 - 백엔드 개발자 | 잡코리아")

        result = worker.extract_jobkorea_fields(html)

        assert result["company_name"] is None
        assert result["title"] == "백엔드 개발자"

    def test_should_join_region_and_locality_when_different(self):
        html = build_html(json_ld=make_jobposting_ld(region="서울", locality="강남구"))

        assert worker.extract_jobkorea_fields(html)["location"] == "서울 강남구"

    @pytest.mark.parametrize(
        "region, locality, expected",
        [
            ("서울", "강남구", "서울 강남구"),
            (None, "강남구", "강남구"),
            ("부산", None, "부산"),
            ("서울", "서울", "서울"),
            (None, None, None),
        ],
    )
    def test_location_resolution_matches_other_parsers(self, region, locality, expected):
        """잡코리아는 else 분기가 `locality or region`, 나머지는 `region or locality` 다.

        표기는 다르지만 결과는 모든 입력에서 같다. else 로 오는 조건이
        "한쪽이 falsy" 또는 "둘이 동일" 뿐이라 우선순위가 관측되지 않기 때문이다.
        따라서 B 리팩터링에서 하나로 합쳐도 안전하다는 것을 여기서 고정한다.
        """
        html = build_html(json_ld=make_jobposting_ld(region=region, locality=locality))

        assert worker.extract_jobkorea_fields(html)["location"] == expected
        assert worker.extract_wanted_fields(html)["location"] == expected

    def test_should_extract_experience_from_meta_description(self):
        html = build_html(meta_description="잡코리아컴퍼니 채용, 경력 : 3년 이상, 서울")

        assert worker.extract_jobkorea_fields(html)["experience_level"] == "3년 이상"

    def test_should_not_read_employment_type_from_json_ld(self):
        """잡코리아 파서는 employmentType을 읽지 않아 항상 None이다."""
        html = build_html(json_ld=make_jobposting_ld(employment_type="FULL_TIME"))

        assert worker.extract_jobkorea_fields(html)["employment_type"] is None

    def test_should_strip_company_prefix_from_title(self):
        html = build_html(
            json_ld=make_jobposting_ld(title="잡코리아컴퍼니 백엔드 개발자", company="잡코리아컴퍼니")
        )

        assert worker.extract_jobkorea_fields(html)["title"] == "백엔드 개발자"

    def test_should_escape_regex_metacharacters_in_company_name(self):
        """'(주)' 처럼 정규식 메타문자가 든 회사명도 안전하게 접두사 제거해야 한다."""
        html = build_html(
            json_ld=make_jobposting_ld(title="(주)테스트 백엔드 개발자", company="(주)테스트")
        )

        assert worker.extract_jobkorea_fields(html)["title"] == "백엔드 개발자"


class TestExtractFieldsByDomain:
    @pytest.mark.parametrize(
        "url, expected_parser",
        [
            ("https://www.wanted.co.kr/wd/242151", "extract_wanted_fields"),
            (
                "https://www.saramin.co.kr/zf_user/jobs/relay/view?rec_idx=1",
                "extract_saramin_fields",
            ),
            ("https://groupby.kr/jobs/123", "extract_groupby_fields"),
            ("https://www.jobkorea.co.kr/Recruit/GI_Read/123", "extract_jobkorea_fields"),
            ("https://www.catch.co.kr/NCS/RecruitInfoDetails/123", "extract_catch_fields"),
        ],
    )
    def test_should_route_url_to_matching_domain_parser(self, url, expected_parser, monkeypatch):
        called = {}

        def spy(*_args):
            called["hit"] = True
            return {field: None for field in CONTRACT_FIELDS}

        monkeypatch.setattr(worker, expected_parser, spy)

        worker.extract_fields_by_domain(url, build_html(title="아무거나"))

        assert called.get("hit") is True

    def test_should_route_by_hostname_regardless_of_case(self):
        html = build_html(og_title="[대문자컴퍼니] 백엔드 개발자 | 캐치")

        result = worker.extract_fields_by_domain("https://WWW.CATCH.CO.KR/detail/1", html)

        assert result["company_name"] == "대문자컴퍼니"

    def test_should_ignore_path_when_routing(self):
        """hostname만 보므로 경로에 다른 사이트 이름이 있어도 영향받지 않는다."""
        html = build_html(og_title="[캐치컴퍼니] 백엔드 개발자 | 캐치")

        result = worker.extract_fields_by_domain(
            "https://www.catch.co.kr/redirect?to=wanted.co.kr", html
        )

        assert result["company_name"] == "캐치컴퍼니"

    def test_should_use_generic_fallback_for_unknown_domain(self):
        html = build_html(
            title="타이틀 태그 제목",
            og_title="og 제목",
            meta_description="알 수 없는 사이트의 공고 설명",
        )

        result = worker.extract_fields_by_domain("https://unknown-job-site.com/posting/1", html)

        assert result["title"] == "og 제목"
        assert result["description_text"] == "알 수 없는 사이트의 공고 설명"
        assert result["company_name"] is None
        assert result["location"] is None

    def test_fallback_should_use_body_text_when_meta_description_missing(self):
        html = build_html(title="제목만 있는 페이지", body="<p>본문에만 있는 공고 내용</p>")

        result = worker.extract_fields_by_domain("https://unknown-job-site.com/posting/1", html)

        assert "본문에만 있는 공고 내용" in result["description_text"]

    def test_fallback_should_truncate_body_text_to_2000_chars(self):
        """본문 전체가 BigQuery로 흘러가는 것을 막는 안전장치."""
        html = build_html(title="긴 본문", body="가" * 5000)

        result = worker.extract_fields_by_domain("https://unknown-job-site.com/posting/1", html)

        assert len(result["description_text"]) == 2000

    def test_fallback_should_keep_contract_fields(self):
        result = worker.extract_fields_by_domain(
            "https://unknown-job-site.com/posting/1", build_html(title="제목")
        )

        assert set(result.keys()) == CONTRACT_FIELDS


class TestMetaExtractionHelpers:
    def test_should_unescape_html_entities_in_meta_content(self):
        html = build_html(meta_description="AT&amp;T 채용 &lt;백엔드&gt;")

        assert worker.extract_meta_content(html, name="description") == "AT&T 채용 <백엔드>"

    def test_should_return_none_when_meta_tag_absent(self):
        assert worker.extract_meta_content(build_html(title="제목"), name="description") is None

    def test_should_return_none_when_neither_name_nor_property_given(self):
        assert worker.extract_meta_content(build_html(meta_description="설명")) is None

    def test_should_extract_title_tag_across_newlines(self):
        html = "<html><head><title>\n  여러 줄\n  제목\n</title></head></html>"

        assert worker.extract_title_tag(html) == "여러 줄\n  제목"

    def test_should_return_none_when_title_tag_unclosed(self):
        assert worker.extract_title_tag("<html><head><title>닫히지 않은") is None

    def test_meta_extraction_fails_when_content_precedes_name_attribute(self):
        """알려진 한계: 정규식이 name/property가 content보다 앞에 온다고 가정한다.

        속성 순서만 바꾼 유효한 HTML에서 값을 못 읽는다. HTML 파서로 교체하면
        이 테스트는 뒤집혀야 한다 — 교체 여부를 판단할 근거로 남겨둔다.
        """
        html = '<html><head><meta content="설명입니다" name="description"></head></html>'

        assert worker.extract_meta_content(html, name="description") is None


class TestStripHtmlTags:
    def test_should_convert_br_to_newline(self):
        assert worker.strip_html_tags("첫 줄<br>둘째 줄") == "첫 줄\n둘째 줄"

    def test_should_convert_block_close_tags_to_newline(self):
        result = worker.strip_html_tags("<p>문단1</p><p>문단2</p>")

        assert "<p>" not in result
        assert "문단1" in result
        assert "문단2" in result

    def test_should_replace_tags_with_space_to_avoid_word_merge(self):
        """태그를 빈 문자열이 아닌 공백으로 치환해 단어가 붙는 것을 막는다."""
        assert worker.strip_html_tags("<span>앞</span><span>뒤</span>") == "앞 뒤"

    def test_should_unescape_entities(self):
        assert worker.strip_html_tags("A &amp; B") == "A & B"

    def test_should_collapse_repeated_blank_lines(self):
        assert "\n\n\n" not in worker.strip_html_tags("<p>A</p><p></p><p></p><p>B</p>")


class TestNormalizeWhitespace:
    def test_should_collapse_internal_whitespace(self):
        assert worker.normalize_whitespace("백엔드   \n 개발자") == "백엔드 개발자"

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_should_return_none_for_empty_input(self, value):
        """빈 문자열이 필드에 들어가지 않게 None으로 정규화한다."""
        assert worker.normalize_whitespace(value) is None


class TestExtractCanonicalUrl:
    def test_should_extract_canonical_href(self):
        html = build_html(canonical="https://www.saramin.co.kr/zf_user/jobs/view?rec_idx=99")

        result = worker.extract_canonical_url(html)

        assert result == "https://www.saramin.co.kr/zf_user/jobs/view?rec_idx=99"

    def test_should_return_none_when_canonical_absent(self):
        assert worker.extract_canonical_url(build_html(title="제목")) is None
