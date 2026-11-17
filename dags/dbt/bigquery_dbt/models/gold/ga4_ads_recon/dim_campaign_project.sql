{{ config(
    tags=["ga4_ads_recon", "gold", "daily"],
) }}

-- campaign_id -> project_name bridge, so dashboards can map a campaign (or a dim_user row, via
-- dim_user.campaign_id) to a Project with one join. Spend can't be split by project from the
-- campaign alone (campaigns carry no project dimension), but a campaign's ad groups all send
-- clicks to one project's domain, so the landing-page URL gives it. Checked against prod: no
-- campaign maps to more than one project.
--
-- mapping_source says how a campaign got its project:
--   ads_landing_page          campaign_name + customer_id matched to ads_LandingPageStats rows
--                             whose landing page domain is a project domain (dim_project.domain).
--                             campaign_name is not unique across campaign_ids, but within a
--                             customer_id it only ever resolves to one project, so the join
--                             cannot fan out to several projects (enforced by the unique test).
--   purchaser_payment_account campaigns the Ads data says nothing about (no landing-page
--                             stats at all -- e.g. no Ads record, or a campaign type that has none)
--                             whose first-time payers' payment accounts ALL belong to a single
--                             project (dim_user.acquisition_source_account ->
--                             seed_ga4_payment_account_map). Left out, not guessed: campaigns
--                             with any payer on a non-project / unmapped account, payers spanning
--                             more than one project, or landing pages on a non-project domain
--                             (Ads evidence contradicting the payer evidence).
-- Where both routes exist the ads_landing_page row wins; for the users both cover they agree
-- 100% in prod.
--
-- Campaigns of non-project products (QR, chatbot, ...) are intentionally absent: a null project
-- on the join means "not one of the dim_project projects", not "unknown".
--
-- Grain: campaign_id (string, same type as dim_user.campaign_id).

with ads_bridge as (

    select
        cast(c.campaign_id as string) as campaign_id,
        n.project_name,
        'ads_landing_page'            as mapping_source
    from (
        select distinct campaign_id, customer_id, campaign_name
        from {{ source('bronze_google_ads', 'ads_Campaign_2747379648') }}
    ) c
    join (
        select distinct l.customer_id, l.campaign_name, p.project_name
        from {{ source('bronze_google_ads', 'ads_LandingPageStats_2747379648') }} l
        join {{ ref('dim_project') }} p
            on p.domain = regexp_extract(l.landing_page_view_unexpanded_final_url, r'https?://(?:www\.)?([^/?#]+)')
    ) n
        on n.customer_id = c.customer_id
        and n.campaign_name = c.campaign_name
    group by 1, 2

),

ads_landing_evidence as (

    -- campaigns that DO have landing-page stats, on any domain. If such a campaign is not in
    -- ads_bridge its landing pages are on non-project domains, which contradicts a project
    -- label coming from its payers, so the payer fallback must not claim it.
    select distinct cast(c.campaign_id as string) as campaign_id
    from {{ source('bronze_google_ads', 'ads_Campaign_2747379648') }} c
    join {{ source('bronze_google_ads', 'ads_LandingPageStats_2747379648') }} l
        on  l.customer_id   = c.customer_id
        and l.campaign_name = c.campaign_name

),

account_project as (

    select distinct m.account_key, p.project_name
    from {{ ref('seed_ga4_payment_account_map') }} m
    join {{ ref('dim_project') }} p on p.ga4_property_name = m.ga4_property_name

),

payer_bridge as (

    select
        d.campaign_id,
        any_value(ap.project_name) as project_name,
        'purchaser_payment_account' as mapping_source
    from {{ ref('dim_user') }} d
    -- left join: payers on a non-project / unmapped account must stay visible so they can veto
    -- the campaign, not silently drop out before the single-project check
    left join account_project ap on ap.account_key = d.acquisition_source_account
    where d.campaign_id is not null
      and d.first_revenue_date is not null
      and d.campaign_id not in (select campaign_id from ads_landing_evidence)
    group by d.campaign_id
    having count(distinct ap.project_name) = 1
       and countif(ap.project_name is null) = 0

)

select campaign_id, project_name, mapping_source from ads_bridge
union all
select campaign_id, project_name, mapping_source from payer_bridge
