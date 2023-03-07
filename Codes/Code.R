
library(sqldf)
library(dplyr)
library(data.table)
library(microbenchmark)

options(stringsAsFactors=FALSE)
Badges <- read.csv("Badges.csv.gz")
Comments <- read.csv("Comments.csv.gz")
PostLinks <- read.csv("PostLinks.csv.gz")
Posts <- read.csv("Posts.csv.gz")
Tags <- read.csv("Tags.csv.gz")
Users <- read.csv("Users.csv.gz")
Votes <- read.csv("Votes.csv.gz")

# 1)
# Zapytanie SQL
df_sql_1 <- function(Tags){
sqldf(
  "SELECT Count, TagName
  FROM Tags
  WHERE Count > 1000
  ORDER BY Count DESC"
)
}

# Zapytanie R base
df_base_1 <- function(Tags){
    wynik <- Tags[Tags$Count >= 1000, c("Count","TagName")]

    wynik <- wynik[order(wynik$Count, decreasing = TRUE),]
    row.names(wynik) <- NULL
    wynik
}

# Zapytanie dplyr
df_dplyr_1 <- function(Tags){
  wynik <- select(filter(Tags, Count > 1000), Count, TagName)
  arrange(wynik, desc(Count))

}

# Zapytanie data.table
df_table_1 <- function(Tags){
  Tags <- as.data.table(Tags)
  
  wynik <- Tags[Count > 1000, .(Count, TagName)]
  wynik[order(-Count)]
}


# 2)

# Zapytanie sql
df_sql_2 <- function(Users, Posts){
  sqldf(
    "SELECT Location, COUNT(*) AS Count
    FROM (
      SELECT Posts.OwnerUserId, Users.Id, Users.Location
      FROM Users
      JOIN Posts ON Users.Id = Posts.OwnerUserId
    )
    WHERE Location NOT IN ('')
    GROUP BY Location
    ORDER BY Count DESC
    LIMIT 10"
  )
}

#Zapytanie R base 
df_base_2 <- function(Users, Posts){
    polaczone <- merge(Users[c("Id", "Location")], Posts["OwnerUserId"], by.x = "Id", by.y = "OwnerUserId")
    lokacja <- polaczone[polaczone$Location != '',]
    aggregate(
      lokacja$Location,
      by = list(lokacja$Location),
      FUN = length
    ) -> wynik
    colnames(wynik) <- c("Location", "Count")
    wynik <- head(wynik[order(wynik$Count, decreasing = TRUE),], 10)
    rownames(wynik) <- NULL
    wynik
    
}

# Zapytanie dplyr
df_dplyr_2 <- function(Users, Posts){
  polaczone <- inner_join(select(Posts, OwnerUserId),select(Users,Id,Location), by = c("OwnerUserId" = "Id"))
  wynik <- 
    filter(polaczone, Location != '') %>%
    count(Location)
  wynik = rename(wynik, Count = n)
  wynik <- arrange(wynik, desc(Count))
  slice(wynik, 1:10)
  
}

# Zapytanie data.table
df_table_2 <- function(Users, Posts){
  Posts <- as.data.table(Posts)
  Users <- as.data.table(Users)
  setkey(Posts, OwnerUserId)
  setkey(Users, Id)
  Posts <- Posts[Users, nomatch = 0, .(Id, Location)]
  wynik <- Posts[Location != '', .(Count = .N), by = Location]
  wynik <- wynik[order(-Count)]
  wynik[,.SD[1:10]]
}

# 3)

# Zapytanie sql
df_sql_3 <- function(Badges){
  sqldf(
    "SELECT Year, SUM(Number) AS TotalNumber
    FROM (
      SELECT
      Name,
      COUNT(*) AS Number,
      STRFTIME('%Y', Badges.Date) AS Year
      FROM Badges
      WHERE Class = 1
      GROUP BY Name, Year
    )
    GROUP BY Year
    ORDER BY TotalNumber"
    
  )
}

# Zapytanie R base
df_base_3 <- function(Badges){
  Year <-strftime(Badges$Date, "%Y")
  Badges2 <- cbind(Badges, Year)
  aggregate(
    Badges2[Badges2$Class == 1, c('Name')],
    Badges2[Badges2$Class == 1, c('Name', 'Year'), drop = FALSE],
    length
    
  ) -> pomocnicze
  names(pomocnicze)[3] <- "Number"

  aggregate(
    pomocnicze[,c("Number")],
    pomocnicze[,"Year", drop = FALSE],
    function(x) sum(x)
  ) -> wynik
  names(wynik)[2] <- "TotalNumber"
  wynik[order(wynik$TotalNumber),]
}




# Zapytanie dplyr
df_dplyr_3 <- function(Badges){
  Year <-strftime(Badges$Date, "%Y")
  Badges <- mutate(Badges, Year = Year)
  wybor <- 
    select(filter(Badges, Class == 1), Year, Name) %>%
    count(Name, Year)
  wybor = rename(wybor, Number = n)
  
  wynik <- 
    select(wybor, Year, Number) %>%
    group_by(Year) %>%
    summarise(TotalNumber = sum(Number))
  wynik
}

# Zapytanie data.table
df_table_3 <- function(Badges){
  Badges <- as.data.table(Badges)
  Badges <- Badges[,Year := strftime(Date, "%Y")] 
  wybor <- Badges[Class == 1, .(Number = .N), by = .(Name,Year)]
  
  wynik <- wybor[,.(TotalNumber = sum(Number)), by = .(Year)][order(TotalNumber)]
  wynik
}

# 4)

# Zapytanie sql
df_sql_4 <- function(Posts, Users){
  sqldf(
    "SELECT
    Users.AccountId,
    Users.DisplayName,
    Users.Location,
    AVG(PostAuth.AnswersCount) as AverageAnswersCount
    FROM
    (
      SELECT
      AnsCount.AnswersCount,
      Posts.Id,
      Posts.OwnerUserId
      FROM (
        SELECT Posts.ParentId, COUNT(*) AS AnswersCount
        FROM Posts
        WHERE Posts.PostTypeId = 2
        GROUP BY Posts.ParentId
      ) AS AnsCount
      JOIN Posts ON Posts.Id = AnsCount.ParentId
    ) AS PostAuth
    JOIN Users ON Users.AccountId=PostAuth.OwnerUserId
    GROUP BY OwnerUserId
    ORDER BY AverageAnswersCount DESC, AccountId ASC
    LIMIT 10"
    
    
  )
}

# Zapytanie R base
df_base_4 <- function(Posts, Users){
  aggregate(
    Posts[Posts$PostTypeId == 2, "ParentId"],
    Posts[Posts$PostTypeId == 2, "ParentId", drop = FALSE],
    length
    ) -> AnsCount
  names(AnsCount)[2] <- "AnswersCount"
  PostAuth <-merge(Posts,AnsCount, by.x = "Id", by.y = "ParentId")
  PostAuth <- PostAuth[,c("AnswersCount", "Id", "OwnerUserId")]
  wynik <- merge(Users, PostAuth, by.x = "AccountId", by.y = "OwnerUserId")
  aggregate(
    wynik[ ,c("AnswersCount")],
    wynik[ ,c("AccountId", "DisplayName", "Location"), drop = FALSE],
    mean
  ) -> wynik
  names(wynik)[4] <- "AverageAnswersCount"
  wynik <- wynik[order( -wynik$AverageAnswersCount, wynik$AccountId),]
  rownames(wynik) <- NULL
  head(wynik,10)
  
  
  
  }

#Zapytanie dplyr
df_dplyr_4 <- function(Posts, Users){
  AnsCount <-
    select(filter(Posts, PostTypeId == 2), ParentId) %>%
    count(ParentId)
  AnsCount = rename(AnsCount, AnswersCount = n)  
  PostAuth <- inner_join(select(Posts, Id, OwnerUserId), select(AnsCount, AnswersCount, ParentId), by = c("Id" = "ParentId"))
  wynik <- inner_join(Users, PostAuth, by = c("AccountId" = "OwnerUserId"))
  wynik <- 
    select(wynik, AccountId, DisplayName, Location, AnswersCount) %>%
    group_by(AccountId, DisplayName, Location) %>%
    summarise(AverageAnswersCount = mean(AnswersCount), .groups = 'drop')
  wynik <- arrange(wynik, desc(AverageAnswersCount), AccountId)
  slice(wynik, 1:10)
              
}

# Zpaytanie data.table
df_table_4 <- function(Posts, Users){
  Posts <- as.data.table(Posts)
  Users <- as.data.table(Users)
  AnsCount <- Posts[PostTypeId == 2, .(AnswersCount = .N), by = ParentId]
  
  setkey(Posts, Id)
  setkey(AnsCount, ParentId)
  
  PostAuth <- Posts[AnsCount, nomatch = 0, .(Id, AnswersCount, OwnerUserId)]
  
  setkey(Users, AccountId)
  setkey(PostAuth, OwnerUserId)
  
  Users <- Users[PostAuth, nomatch = 0]
  wynik <- Users[,.(AverageAnswersCount = mean(AnswersCount)), by = .(AccountId, DisplayName, Location)][order(-AverageAnswersCount, AccountId)]
  wynik[,.SD[1:10]] 
}

# 5)

# Zapytanie sql
df_sql_5 <- function(Posts, Votes){
  sqldf(
        "SELECT Posts.Title, Posts.Id,
        STRFTIME('%Y-%m-%d', Posts.CreationDate) AS Date,
        VotesByAge.Votes
        FROM Posts
        JOIN (
          SELECT
          PostId,
          MAX(CASE WHEN VoteDate = 'new' THEN Total ELSE 0 END) NewVotes,
          MAX(CASE WHEN VoteDate = 'old' THEN Total ELSE 0 END) OldVotes,
          SUM(Total) AS Votes
          FROM (
            SELECT
            PostId,
            CASE STRFTIME('%Y', CreationDate)
            WHEN '2021' THEN 'new'
            WHEN '2020' THEN 'new'
            ELSE 'old'
            END VoteDate,
            COUNT(*) AS Total
            FROM Votes
            WHERE VoteTypeId IN (1, 2, 5)
            GROUP BY PostId, VoteDate
          ) AS VotesDates
          GROUP BY VotesDates.PostId
          HAVING NewVotes > OldVotes
        ) AS VotesByAge ON Posts.Id = VotesByAge.PostId
        WHERE Title NOT IN ('')
        ORDER BY Votes DESC
        LIMIT 10")
}


# Zapytanie R base
df_base_5 <- function(Posts,Votes){
  CreationDate <- strftime(Votes$CreationDate, "%Y")
  VoteDate <- ifelse(CreationDate == "2021" | CreationDate == "2020", "new", "old")
  VotesDates <- cbind(Votes, VoteDate)
  aggregate(
    VotesDates[VotesDates$VoteTypeId == 1 | VotesDates$VoteTypeId == 2 | VotesDates$VoteTypeId == 5, "VoteDate"],
    VotesDates[VotesDates$VoteTypeId == 1 | VotesDates$VoteTypeId == 2 | VotesDates$VoteTypeId == 5, c("VoteDate","PostId")],
    length
  ) -> VotesDates
  names(VotesDates)[3] <- "Total"

  NewVotes <- ifelse(VotesDates$VoteDate == "new", VotesDates$Total, 0)
  OldVotes <- ifelse(VotesDates$VoteDate == "old", VotesDates$Total, 0)
  NewVotes <- as.data.frame(cbind(VotesDates$PostId,VotesDates$Total ,NewVotes, OldVotes))
  names(NewVotes)[1:2] <- c("PostId", "Total")
  aggregate(
    NewVotes[,c("NewVotes", "OldVotes")],
    NewVotes[,c("PostId"), drop = FALSE],
    function(x) max(x)
  ) -> MaxVotes
  names(MaxVotes)[2:3] <- c("NewVotes", "OldVotes")
  aggregate(
    NewVotes[,c("Total")],
    NewVotes[,c("PostId"), drop = FALSE],
    function(x) sum(x)
  ) -> SumTotal
  names(SumTotal)[2] <- "Votes"

  VotesByAge <- merge(MaxVotes, SumTotal, by = "PostId")
  VotesByAge <- VotesByAge[VotesByAge$NewVotes > VotesByAge$OldVotes,]
  rownames(VotesByAge) <- NULL
  as.integer(VotesByAge$PostId) -> VotesByAge$PostId
  as.integer(VotesByAge$Votes) -> VotesByAge$Votes

  wynik <- merge(Posts, VotesByAge, by.x = "Id", by.y = "PostId")
  Date <- strftime(wynik$CreationDate, "%Y-%m-%d")
  wynik <- cbind(wynik, Date)
  wynik <- wynik[wynik$Title != '',c("Title", "Id", "Date", "Votes")]
  wynik <- wynik[order(-wynik$Votes), ]
  wynik <- head(wynik, 10)
  rownames(wynik) <- NULL
  wynik
}


#Zapytanie dplyr
df_dplyr_5 <- function(Posts, Votes){
  Data <- strftime(Votes$CreationDate, "%Y")
  Votes <-
    Votes %>%
    mutate(VoteDate = if_else(Data == 2021 | Data == 2020, 'new', 'old'))
  VotesDates <- 
    select(filter(Votes, VoteTypeId %in% c(1,2,5)), PostId, VoteDate) %>%
    group_by(PostId, VoteDate)%>%
    count(VoteDate)
  VotesDates <- rename(VotesDates, Total = n)
  VotesDates <- 
    VotesDates %>%
    mutate(NewVotes = if_else(VoteDate == 'new', Total, as.integer(0)))
  VotesDates <- 
    VotesDates %>%
    mutate(OldVotes = if_else(VoteDate == 'old', Total, as.integer(0)))
  VotesByAge <- 
    select(VotesDates, PostId, NewVotes, OldVotes, Total) %>%
    group_by(PostId) %>%
    summarise(NewVotes = max(NewVotes), OldVotes = max(OldVotes), Votes = sum(Total))

  VotesByAge <- filter(VotesByAge, NewVotes > OldVotes)
  
  Posts <- inner_join(Posts, VotesByAge, by = c("Id" = "PostId"))
  
  Posts <- 
    Posts %>%
    mutate(Date = strftime(CreationDate, '%Y-%m-%d'))
  
  wynik <- select(filter(Posts, Title != ''), Title, Id, Date, Votes)
  wynik <- arrange(wynik, desc(Votes))
  slice(wynik, 1:10)
  
}

# Zapytanie data.table
df_table_5 <- function(Posts, Votes){
  Posts <- as.data.table(Posts)
  Votes <- as.data.table(Votes)
  Votes <- Votes[,VoteDate := strftime(CreationDate, "%Y")]
  Votes <- Votes[VoteDate == "2021" | VoteDate == "2020", VoteDate := "new"]
  Votes <- Votes[VoteDate != "new", VoteDate := "old"]
  
  VotesDates <- Votes[VoteTypeId %in% c(1,2,5), .(Total = .N), by = .(PostId, VoteDate)]
  VotesDates <- VotesDates[VoteDate != 'new', NewVotes := 0]
  VotesDates <- VotesDates[VoteDate == 'new', NewVotes := Total]
  VotesDates <- VotesDates[VoteDate != 'old', OldVotes := 0]
  VotesDates <- VotesDates[VoteDate == 'old', OldVotes := Total]
  
  VotesByAge <- VotesDates[, .(NewVotes = max(NewVotes), OldVotes = max(OldVotes), Votes = sum(Total)), by = .(PostId)]
  VotesByAge <- VotesByAge[NewVotes > OldVotes]
  
  setkey(Posts, Id)
  setkey(VotesByAge, PostId)
  
  Posts <- Posts[VotesByAge, nomatch = 0]
  Posts <- Posts[, Date := strftime(CreationDate, "%Y-%m-%d")]
  
  wynik <- Posts[Title != '', .(Title, Id, Date, Votes)][order(-Votes)]
  wynik[,.SD[1:10]]
  
  
}
