    WITH event_data AS (
        SELECT event_url,
               MAX(CASE WHEN attribute = 'event_name' THEN value END) AS event_name,
               MAX(CASE WHEN attribute = 'event_city' THEN value END) AS event_city,
               MAX(CASE WHEN attribute = 'event_country' THEN value END) AS event_country,
               MAX(CASE WHEN attribute = 'event_start_date' THEN value END) AS event_start_date,
               MAX(CASE WHEN attribute = 'event_industry' THEN value END) AS event_industry
        FROM event_attributes
        GROUP BY event_url
    ),
    company_data AS (
        SELECT company_url,
               MAX(CASE WHEN attribute = 'company_name' THEN value END) AS company_name,
               MAX(CASE WHEN attribute = 'company_country' THEN value END) AS company_country,
               MAX(CASE WHEN attribute = 'company_industry' THEN value END) AS company_industry,
               MAX(CASE WHEN attribute = 'company_revenue' THEN value END) AS company_revenue
        FROM company_attributes
        GROUP BY company_url
    ),
    people_data AS (
        SELECT person_id,
               MAX(CASE WHEN attribute = 'company_url' THEN value END) AS company_url,
               MAX(CASE WHEN attribute = 'person_first_name' THEN value END) AS person_first_name,
               MAX(CASE WHEN attribute = 'person_last_name' THEN value END) AS person_last_name,
               MAX(CASE WHEN attribute = 'person_email' THEN value END) AS person_email,
               MAX(CASE WHEN attribute = 'person_seniority' THEN value END) AS person_seniority,
               MAX(CASE WHEN attribute = 'person_department' THEN value END) AS person_department
        FROM people_attributes
        GROUP BY person_id
    )


    
    SELECT DISTINCT event_city,event_start_date, event_name, event_country, company_industry, company_name, company_url, person_first_name, person_last_name, person_seniority
FROM attendees JOIN event_data USING (event_url) JOIN company_data USING (company_url) LEFT JOIN company_contacts USING (company_url) LEFT JOIN people_data  USING (company_url)
WHERE event_city IN ('San Francisco') AND event_start_date <= '2025-01-31' AND event_start_date >= '2020-01-31'
;