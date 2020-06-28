---
title: "Analysis Plan"
author: "Boyd Nguyen"
date: "10/06/2020"
output:
  html_document: default
  pdf_document: default
---

```{r setup, include=FALSE}
knitr::opts_chunk$set(echo = TRUE)
```

# Background
This is a summary of all the statistical analyses carried out by me, using open datasets from https://openpsychologydata.metajnl.com/articles/10.5334/jopd.35/. The purpose of this paper is to replicate the findings in this study, as well as investigating other hypotheses left unanswered by the original authors.

# Analysis

## Data preparation

``` {r message = FALSE, echo = FALSE}
setwd("C:/Users/boydn/Desktop/Study/Data Analysis Projects/Web-based Positive Psychology Interventions/Data")
```

The following packages are loaded.
``` {r message = FALSE, warning = FALSE}
library(tidyverse)
library(dplyr)
library(ggplot2)
library(psych)
library(readr)
library(forcats)
```

Then, the dataset is loaded, with columns parsed according to variable types.
``` {r message = FALSE, warning = FALSE}
ppinfo <- read_csv("participant-info.csv", 
                   col_types = cols(
                               intervention = col_factor(),
                               sex = col_factor(),
                               educ = col_factor(),
                               income = col_factor()
                                        ))
```

In the original dataset, categorical variables are coded as integers. For ease of understanding, I gave explicit names to the variables' levels. 

``` {r message = FALSE, warning = FALSE}
levels(ppinfo$sex) <- c("Male", "Female")
levels(ppinfo$income) <- c("Above average", "Below average", "Average")
levels(ppinfo$intervention) <- c("Control", "Strengths", "3GT", "Gratitude")
levels(ppinfo$educ) <- c("Postgrad", "Below year 12", "Bachelors", "Year 12", "Vocational")
```

## Descriptives

### Descriptive statistics for all participant variables.
``` {r}
ppinfo_descriptives <- summary(ppinfo[,2:6])
gender_age <- by(ppinfo$age, ppinfo$sex, describe)

ppinfo_descriptives

gender_age
```

There are considerably more females than males. As a result, the findings might not be representative of both sexes.

### Inspection of age distribution

``` {r echo = FALSE}
age_hist <- ggplot(ppinfo, aes(age)) + 
    theme(legend.position = "none", panel.grid = element_blank(), panel.grid.major.y = element_line(colour = "Light gray")) +
    geom_histogram(aes(y = ..density..), binwidth = 5, fill = "White", colour = "Black") +
    stat_function(fun = dnorm, args = list(mean = mean(ppinfo$age, na.rm = TRUE), sd = sd(ppinfo$age, na.rm = TRUE)), colour = "Black") + 
    labs(x = "Age", y = "Frequency", title = "Distribution of participant age")

age_hist
```

### Inspection of income distribution

``` {r echo = FALSE}
income_bar <- ggplot(ppinfo, aes(income)) +
    theme(legend.position = "none", panel.grid = element_blank(), panel.grid.major.y = element_line(colour = "Light gray")) +
    geom_bar(fill = "White", colour = "Black", width = 0.5) +
    labs(x = "Levels of income", y = "Count", title = "Counts of income types")  +
    scale_y_continuous(breaks = seq(0,150, by = 20))

income_bar
``` 

### Inspection of education distribution

``` {r echo = FALSE}

edu_bar <- ggplot(ppinfo, aes(ppinfo$educ)) +
  theme(legend.position = "none", panel.grid = element_blank(), panel.grid.major.y = element_line(colour = "Light gray")) +
  geom_bar(fill = "White", colour = "Black", width = 0.5) +
  labs(x = "Levels of education", y = "Count", title = "Counts of education levels") 

edu_bar
```

### Equal assignment checks

To check whether there are equal proportions of males and females in each intervention group, I conducted a Chi-square Goodness-of-Fit Test on the number of males and females in each intervention.

``` {r}
sex_intervention <- table(ppinfo$intervention,ppinfo$sex)
sex_group_prop <- round(prop.table(sex_intervention, margin =2)*100)
chisq <- chisq.test(sex_intervention)

chisq
```
Chi-square test is not significant. This means that there are equal proportions of sexes in each intervention.

To check whether there are equal proportions of income levels in each intervention group, I conducted another Chi-square Goodness-of-Fit Test. My rationale behind the test is the possibility that higher income people are less affected by the interventions.

``` {r}
income_intervention <- table(ppinfo$intervention,ppinfo$income)
chisq2 <- chisq.test(income_intervention)

chisq2
```
Chi-square test is not significant, indicating that there are equal proportions of income levels in each intervention.

### Measurement data - number of participants completing measurement scales

``` {r echo = FALSE}
data <- read_csv("ahi-cesd.csv",
                 col_type = cols(
                            occasion = col_factor()
                 ))

levels(data$occasion) <- c("0 - Pretest", "1 - Posttest", "2 - One-week follow-up", "3 - One-month follow-up", "4 - Three-month follow-up", "5 - Six-month follow-up")

data_filter <- data %>%
  select(id,occasion,intervention,ahiTotal,cesdTotal)

data_filter %>% 
  group_by(occasion) %>%
  summarise(
    count = n()
  )

sum_pretest <- sum(data$occasion == "0 - Pretest")
sum_posttest <- sum(data$occasion == "1 - Posttest")
drop <- ((sum_pretest - sum_posttest)/sum_pretest) * 100
```

There was a `r abs(round(drop,0))`% drop in responses post-intervention. This calls for an investigation into who was more likely to drop out, which might affect the study's validity.
