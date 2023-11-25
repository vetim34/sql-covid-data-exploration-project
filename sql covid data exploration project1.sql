-- SQL Project 1
--- creating the covid deaths table
CREATE TABLE cov_deaths (
    iso_code VARCHAR(50),
    continent VARCHAR(50),
    location VARCHAR(50),
    date DATE,
    population numeric,
    total_cases numeric,
    new_cases numeric,
    new_cases_smoothed FLOAT,
    total_deaths numeric,
    new_deaths numeric,
    new_deaths_smoothed FLOAT,
    total_cases_per_million FLOAT,
    new_cases_per_million FLOAT,
    new_cases_smoothed_per_million FLOAT,
    total_deaths_per_million FLOAT,
    new_deaths_per_million FLOAT,
    new_deaths_smoothed_per_million FLOAT,
    reproduction_rate FLOAT,
    icu_patients numeric,
    icu_patients_per_million FLOAT,
    hosp_patients numeric,
    hosp_patients_per_million FLOAT,
    weekly_icu_admissions numeric,
    weekly_icu_admissions_per_million FLOAT,
    weekly_hosp_admissions numeric,
    weekly_hosp_admissions_per_million FLOAT
);

-- importing dataset using query
copy public.cov_deaths
from 'C:\Users\USER\Desktop\10alytics\Alex Bootcamp\covid deaths.csv'
delimiter ',' csv header;

-- query to show table content
select *
from cov_deaths;


-- creating a table for covid vaccinations
CREATE TABLE cov_vax (
    iso_code VARCHAR(30),
    continent VARCHAR(50),
    location VARCHAR(50),
    date DATE,
    total_tests numeric,
    new_tests numeric,
    total_tests_per_thousand FLOAT,
    new_tests_per_thousand FLOAT,
    new_tests_smoothed FLOAT,
    new_tests_smoothed_per_thousand FLOAT,
    positive_rate FLOAT,
    tests_per_case FLOAT,
    tests_units VARCHAR(50),
    total_vaccinations numeric,
    people_vaccinated numeric,
    people_fully_vaccinated numeric,
    total_boosters numeric,
    new_vaccinations numeric,
    new_vaccinations_smoothed FLOAT,
    total_vaccinations_per_hundred FLOAT,
    people_vaccinated_per_hundred FLOAT,
    people_fully_vaccinated_per_hundred FLOAT,
    total_boosters_per_hundred FLOAT,
    new_vaccinations_smoothed_per_million FLOAT,
    new_people_vaccinated_smoothed numeric,
    new_people_vaccinated_smoothed_per_hundred FLOAT,
    stringency_index FLOAT,
    population_density FLOAT,
    median_age FLOAT,
    aged_65_older FLOAT,
    aged_70_older FLOAT,
    gdp_per_capita FLOAT,
    extreme_poverty FLOAT,
    cardiovasc_death_rate FLOAT,
    diabetes_prevalence FLOAT,
    female_smokers FLOAT,
    male_smokers FLOAT,
    handwashing_facilities FLOAT,
    hospital_beds_per_thousand FLOAT,
    life_expectancy FLOAT,
    human_development_index FLOAT,
    excess_mortality_cumulative_absolute float,
    excess_mortality_cumulative FLOAT,
    excess_mortality FLOAT,
    excess_mortality_cumulative_per_million FLOAT
);

-- importing dataset using query
copy public.cov_vax
from 'C:\Users\USER\Desktop\10alytics\Alex Bootcamp\covid vaccinations.csv'
delimiter ',' csv header;

-- query to show table content
select *
from cov_vax;

-- Querying the Datasets
select location, date, total_cases, new_cases, total_deaths, population
from cov_deaths;

-- Looking at Total Caes vs Total Deaths
--- likelihood of covid deaths in Nigeria
select location, date, total_cases, total_deaths, (total_deaths / total_cases)*100 as deathrate
from cov_deaths
where location like '%Nigeria%'
order by 1,2;

-- Looking at total cases against population
--- population that got covid
select location, date, total_cases, population, (total_cases / population)*100 as covid_infection_percent
from cov_deaths
where location like '%Nigeria%'
order by 1,2;

-- Looking at countries with highest infection rate
select location, population, max(total_cases) highest_infection_count, max(total_cases / population)*100 as infection_rate
from cov_deaths
--where location like '%Nigeria%'
where continent is not null and total_cases is not null
group by 1,2
order by 4 desc;

-- countries with highest death count per population
select location, max(cast(total_deaths as int)) totaldeathcount
from cov_deaths
where continent is not null and total_deaths is not null
group by 1
order by 2 desc;

-- Showing Continents with Highest death per population 
-- if we break this down by continents
select continent, max(cast(total_deaths as int)) totaldeathcount
from cov_deaths
where continent is not null and total_deaths is not null
group by 1
order by 2 desc;

-- Global Numbers
select date, sum(new_cases) cases, sum(new_deaths) deaths
from cov_deaths
where continent is not null
group by 1
--order by 4 desc;

SELECT -- another way of writing the above
    date,
    SUM(new_cases) AS cases,
    SUM(new_deaths) AS deaths,
    CASE
        WHEN SUM(new_cases) = 0 THEN NULL
        ELSE SUM(new_deaths)::numeric / NULLIF(SUM(new_cases), 0)
    END AS death_rate
FROM
    cov_deaths
WHERE
    continent IS NOT NULL
GROUP BY
    date
---------
-- checking total cases, deaths and total death rate
select sum(new_cases) totalcases, sum(new_deaths) totaldeaths, sum(new_deaths)/nullif(sum(new_cases),0) as totaldeathrate
from cov_deaths
where continent is not null
;

--- joining both tables
select *
from cov_deaths cd
join cov_vax cv
on cd.location = cv.location and cd.date = cv.date;

-- Looking at Total Population n vaccination
-- i.e total amount of people that have been vaccinated
select cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations
from cov_deaths cd
join cov_vax cv
on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null and new_vaccinations is not null
order by 2,3;

-- if we wanted to use the partition by
-- so basically, underthe people vaxxed column, you can tell how many are vaxed in total from first day to that one
select cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations,
sum(cv.new_vaccinations) over(partition by cd.location order by cd.location, cd.date) as rolling_people_vaxxed
from cov_deaths cd
join cov_vax cv
on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null and new_vaccinations is not null
order by 2,3;


-- total people against vaccination in a country
-- here we use a cte or temp table

-- using cte
with pop_vs_vax(continent, location, date, population, new_vaccinations, people_vax)
as
(
select cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations,
sum(cv.new_vaccinations) over(partition by cd.location order by cd.location, cd.date) as people_vaxxed
from cov_deaths cd
join cov_vax cv
on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null and new_vaccinations is not null
--order by 2,3
)
select *
from pop_vs_vax;

-- using temp table
drop table if exists percentpopulationvaccinated
create table percentpopulationvaccinated
(
continent varchar(255),
location varchar(255),
date date,
population numeric,
new_vaccinations numeric,
rollingpeoplevaxin numeric
)
insert into percentpopulationvaccinated
select cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations,
sum(cv.new_vaccinations) over(partition by cd.location order by cd.location, cd.date) as people_vaxxed
from cov_deaths cd
join cov_vax cv
on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null and new_vaccinations is not null
--order by 2,3
select *, (rollingpeoplevaxin/population)*100 rollingvaxpercent
from percentpopulationvaccinated



-- creating view to store data for later visualisations
create view percentpopulationvaccinate as
select cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations,
sum(cv.new_vaccinations) over(partition by cd.location order by cd.location, cd.date) as people_vaxxed
from cov_deaths cd
join cov_vax cv
on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null and new_vaccinations is not null
