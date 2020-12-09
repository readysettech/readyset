--
-- Table structure for table `ActionLog`
--


CREATE TABLE `ActionLog` (
  `logId` int(11) NOT NULL AUTO_INCREMENT,
  `contactId` int(11) NOT NULL,
  `paperId` int(11) DEFAULT NULL,
  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `ipaddr` varbinary(32) DEFAULT NULL,
  `action` varbinary(4096) NOT NULL,
  PRIMARY KEY (`logId`),
  KEY `contactId` (`contactId`),
  KEY `paperId` (`paperId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `Capability`
--


CREATE TABLE `Capability` (
  `capabilityType` int(11) NOT NULL,
  `contactId` int(11) NOT NULL,
  `paperId` int(11) NOT NULL,
  `timeExpires` int(11) NOT NULL,
  `salt` varbinary(255) NOT NULL,
  `data` varbinary(4096) DEFAULT NULL,
  PRIMARY KEY (`salt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `ContactInfo`
--


CREATE TABLE `ContactInfo` (
  `contactId` int(11) NOT NULL AUTO_INCREMENT,
  `firstName` varchar(60) NOT NULL DEFAULT '',
  `lastName` varchar(60) NOT NULL DEFAULT '',
  `unaccentedName` varchar(120) NOT NULL DEFAULT '',
  `email` varchar(120) NOT NULL,
  `preferredEmail` varchar(120) DEFAULT NULL,
  `affiliation` varchar(2048) NOT NULL DEFAULT '',
  `voicePhoneNumber` varchar(256) DEFAULT NULL,
  `country` varbinary(256) DEFAULT NULL,
  `password` varbinary(2048) NOT NULL,
  `passwordTime` int(11) NOT NULL DEFAULT '0',
  `passwordUseTime` int(11) NOT NULL DEFAULT '0',
  `collaborators` varbinary(8192) DEFAULT NULL,
  `creationTime` int(11) NOT NULL DEFAULT '0',
  `updateTime` int(11) NOT NULL DEFAULT '0',
  `lastLogin` int(11) NOT NULL DEFAULT '0',
  `defaultWatch` int(11) NOT NULL DEFAULT '2',
  `roles` tinyint(1) NOT NULL DEFAULT '0',
  `disabled` tinyint(1) NOT NULL DEFAULT '0',
  `contactTags` varbinary(4096) DEFAULT NULL,
  `data` varbinary(32767) DEFAULT NULL,
  PRIMARY KEY (`contactId`),
  UNIQUE KEY `rolesContactId` (`roles`,`contactId`),
  UNIQUE KEY `email` (`email`),
  KEY `fullName` (`lastName`,`firstName`,`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `FilteredDocument`
--


CREATE TABLE `FilteredDocument` (
  `inDocId` int(11) NOT NULL,
  `filterType` int(11) NOT NULL,
  `outDocId` int(11) NOT NULL,
  `createdAt` int(11) NOT NULL,
  PRIMARY KEY (`inDocId`,`filterType`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `Formula`
--


CREATE TABLE `Formula` (
  `formulaId` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(200) NOT NULL,
  `heading` varchar(200) NOT NULL DEFAULT '',
  `headingTitle` varbinary(4096) NOT NULL,
  `expression` varbinary(4096) NOT NULL,
  `createdBy` int(11) NOT NULL DEFAULT '0',
  `timeModified` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`formulaId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `MailLog`
--


CREATE TABLE `MailLog` (
  `mailId` int(11) NOT NULL AUTO_INCREMENT,
  `recipients` varbinary(200) NOT NULL,
  `q` varbinary(4096) DEFAULT NULL,
  `t` varbinary(200) DEFAULT NULL,
  `paperIds` blob,
  `cc` blob,
  `replyto` blob,
  `subject` blob,
  `emailBody` blob,
  `fromNonChair` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`mailId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `Mimetype`
--


CREATE TABLE `Mimetype` (
  `mimetypeid` int(11) NOT NULL,
  `mimetype` varbinary(200) NOT NULL,
  `extension` varbinary(10) DEFAULT NULL,
  `description` varbinary(200) DEFAULT NULL,
  `inline` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`mimetypeid`),
  UNIQUE KEY `mimetype` (`mimetype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `Paper`
--


CREATE TABLE `Paper` (
  `paperId` int(11) NOT NULL AUTO_INCREMENT,
  `title` varbinary(256) DEFAULT NULL,
  `authorInformation` varbinary(8192) DEFAULT NULL,
  `abstract` varbinary(16384) DEFAULT NULL,
  `collaborators` varbinary(8192) DEFAULT NULL,
  `timeSubmitted` int(11) NOT NULL DEFAULT '0',
  `timeWithdrawn` int(11) NOT NULL DEFAULT '0',
  `timeFinalSubmitted` int(11) NOT NULL DEFAULT '0',
  `timeModified` int(11) NOT NULL DEFAULT '0',
  `paperStorageId` int(11) NOT NULL DEFAULT '0',
  # `sha1` copied from PaperStorage to reduce joins
  `sha1` varbinary(64) NOT NULL DEFAULT '',
  `finalPaperStorageId` int(11) NOT NULL DEFAULT '0',
  `blind` tinyint(1) NOT NULL DEFAULT '1',
  `outcome` tinyint(1) NOT NULL DEFAULT '0',
  `leadContactId` int(11) NOT NULL DEFAULT '0',
  `shepherdContactId` int(11) NOT NULL DEFAULT '0',
  `managerContactId` int(11) NOT NULL DEFAULT '0',
  `capVersion` int(1) NOT NULL DEFAULT '0',
  # next 3 fields copied from PaperStorage to reduce joins
  `size` int(11) NOT NULL DEFAULT '0',
  `mimetype` varbinary(80) NOT NULL DEFAULT '',
  `timestamp` int(11) NOT NULL DEFAULT '0',
  `pdfFormatStatus` int(11) NOT NULL DEFAULT '0',
  `withdrawReason` varbinary(1024) DEFAULT NULL,
  `paperFormat` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`paperId`),
  KEY `timeSubmitted` (`timeSubmitted`),
  KEY `leadContactId` (`leadContactId`),
  KEY `shepherdContactId` (`shepherdContactId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `PaperComment`
--


CREATE TABLE `PaperComment` (
  `paperId` int(11) NOT NULL,
  `commentId` int(11) NOT NULL AUTO_INCREMENT,
  `contactId` int(11) NOT NULL,
  `timeModified` int(11) NOT NULL,
  `timeNotified` int(11) NOT NULL DEFAULT '0',
  `timeDisplayed` int(11) NOT NULL DEFAULT '0',
  `comment` varbinary(32767) DEFAULT NULL,
  `commentType` int(11) NOT NULL DEFAULT '0',
  `replyTo` int(11) NOT NULL,
  `paperStorageId` int(11) NOT NULL DEFAULT '0',
  `ordinal` int(11) NOT NULL DEFAULT '0',
  `authorOrdinal` int(11) NOT NULL DEFAULT '0',
  `commentTags` varbinary(1024) DEFAULT NULL,
  `commentRound` int(11) NOT NULL DEFAULT '0',
  `commentFormat` tinyint(1) DEFAULT NULL,
  `commentOverflow` longblob,
  PRIMARY KEY (`paperId`,`commentId`),
  UNIQUE KEY `commentId` (`commentId`),
  KEY `contactId` (`contactId`),
  KEY `timeModifiedContact` (`timeModified`,`contactId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `PaperConflict`
--


CREATE TABLE `PaperConflict` (
  `paperId` int(11) NOT NULL,
  `contactId` int(11) NOT NULL,
  `conflictType` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`contactId`,`paperId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `PaperOption`
--


CREATE TABLE `PaperOption` (
  `paperId` int(11) NOT NULL,
  `optionId` int(11) NOT NULL,
  `value` int(11) NOT NULL DEFAULT '0',
  `data` varbinary(32767) DEFAULT NULL,
  `dataOverflow` longblob,
  PRIMARY KEY (`paperId`,`optionId`,`value`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `PaperReview`
--


CREATE TABLE `PaperReview` (
  `paperId` int(11) NOT NULL,
  `reviewId` int(11) NOT NULL AUTO_INCREMENT,
  `contactId` int(11) NOT NULL,
  `reviewToken` int(11) NOT NULL DEFAULT '0',
  `reviewType` tinyint(1) NOT NULL DEFAULT '0',
  `reviewRound` int(1) NOT NULL DEFAULT '0',
  `requestedBy` int(11) NOT NULL DEFAULT '0',
  `timeRequested` int(11) NOT NULL DEFAULT '0',
  `timeRequestNotified` int(11) NOT NULL DEFAULT '0',
  `reviewBlind` tinyint(1) NOT NULL DEFAULT '1',
  `reviewModified` int(1) DEFAULT NULL,
  `reviewAuthorModified` int(1) DEFAULT NULL,
  `reviewSubmitted` int(1) DEFAULT NULL,
  `reviewNotified` int(1) DEFAULT NULL,
  `reviewAuthorNotified` int(11) NOT NULL DEFAULT '0',
  `reviewAuthorSeen` int(1) DEFAULT NULL,
  `reviewOrdinal` int(1) DEFAULT NULL,
  `timeDisplayed` int(11) NOT NULL DEFAULT '0',
  `timeApprovalRequested` int(11) NOT NULL DEFAULT '0',
  `reviewEditVersion` int(1) NOT NULL DEFAULT '0',
  `reviewNeedsSubmit` tinyint(1) NOT NULL DEFAULT '1',
  `overAllMerit` tinyint(1) NOT NULL DEFAULT '0',
  `reviewerQualification` tinyint(1) NOT NULL DEFAULT '0',
  `novelty` tinyint(1) NOT NULL DEFAULT '0',
  `technicalMerit` tinyint(1) NOT NULL DEFAULT '0',
  `interestToCommunity` tinyint(1) NOT NULL DEFAULT '0',
  `longevity` tinyint(1) NOT NULL DEFAULT '0',
  `grammar` tinyint(1) NOT NULL DEFAULT '0',
  `likelyPresentation` tinyint(1) NOT NULL DEFAULT '0',
  `suitableForShort` tinyint(1) NOT NULL DEFAULT '0',
  `paperSummary` mediumblob,
  `commentsToAuthor` mediumblob,
  `commentsToPC` mediumblob,
  `commentsToAddress` mediumblob,
  `weaknessOfPaper` mediumblob,
  `strengthOfPaper` mediumblob,
  `potential` tinyint(4) NOT NULL DEFAULT '0',
  `fixability` tinyint(4) NOT NULL DEFAULT '0',
  `textField7` mediumblob,
  `textField8` mediumblob,
  `reviewWordCount` int(11) DEFAULT NULL,
  `reviewFormat` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`paperId`,`reviewId`),
  UNIQUE KEY `reviewId` (`reviewId`),
  UNIQUE KEY `contactPaper` (`contactId`,`paperId`),
  KEY `paperId` (`paperId`,`reviewOrdinal`),
  KEY `reviewSubmittedContact` (`reviewSubmitted`,`contactId`),
  KEY `reviewNeedsSubmit` (`reviewNeedsSubmit`),
  KEY `reviewType` (`reviewType`),
  KEY `reviewRound` (`reviewRound`),
  KEY `requestedBy` (`requestedBy`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `PaperReviewPreference`
--


CREATE TABLE `PaperReviewPreference` (
  `paperId` int(11) NOT NULL,
  `contactId` int(11) NOT NULL,
  `preference` int(4) NOT NULL DEFAULT '0',
  `expertise` int(4) DEFAULT NULL,
  PRIMARY KEY (`paperId`,`contactId`),
  UNIQUE KEY `contactPaper` (`contactId`,`paperId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `PaperReviewRefused`
--


CREATE TABLE `PaperReviewRefused` (
  `paperId` int(11) NOT NULL,
  `contactId` int(11) NOT NULL,
  `requestedBy` int(11) NOT NULL,
  `reason` varbinary(32767) DEFAULT NULL,
  KEY `paperId` (`paperId`),
  KEY `contactId` (`contactId`),
  KEY `requestedBy` (`requestedBy`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `PaperStorage`
--


CREATE TABLE `PaperStorage` (
  `paperId` int(11) NOT NULL,
  `paperStorageId` int(11) NOT NULL AUTO_INCREMENT,
  `timestamp` int(11) NOT NULL,
  `mimetype` varbinary(80) NOT NULL DEFAULT '',
  `mimetypeid` int(11) NOT NULL DEFAULT '0',
  `paper` longblob,
  `compression` tinyint(1) NOT NULL DEFAULT '0',
  `sha1` varbinary(64) NOT NULL DEFAULT '',
  `documentType` int(3) NOT NULL DEFAULT '0',
  `filename` varbinary(255) DEFAULT NULL,
  `infoJson` varbinary(32768) DEFAULT NULL,
  `size` bigint(11) DEFAULT NULL,
  `filterType` int(3) DEFAULT NULL,
  `originalStorageId` int(11) DEFAULT NULL,
  PRIMARY KEY (`paperId`,`paperStorageId`),
  UNIQUE KEY `paperStorageId` (`paperStorageId`),
  KEY `byPaper` (`paperId`,`documentType`,`timestamp`,`paperStorageId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `PaperTag`
--


CREATE TABLE `PaperTag` (
  `paperId` int(11) NOT NULL,
  `tag` varchar(80) NOT NULL,		# case-insensitive; see TAG_MAXLEN in init.php
  `tagIndex` float NOT NULL DEFAULT '0',
  PRIMARY KEY (`paperId`,`tag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `PaperTagAnno`
--


CREATE TABLE `PaperTagAnno` (
  `tag` varchar(80) NOT NULL,   # case-insensitive; see TAG_MAXLEN in init.php
  `annoId` int(11) NOT NULL,
  `tagIndex` float NOT NULL DEFAULT '0',
  `heading` varbinary(8192) DEFAULT NULL,
  `annoFormat` tinyint(1) DEFAULT NULL,
  `infoJson` varbinary(32768) DEFAULT NULL,
  PRIMARY KEY (`tag`,`annoId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `PaperTopic`
--


CREATE TABLE `PaperTopic` (
  `paperId` int(11) NOT NULL,
  `topicId` int(11) NOT NULL,
  PRIMARY KEY (`paperId`,`topicId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `PaperWatch`
--


CREATE TABLE `PaperWatch` (
  `paperId` int(11) NOT NULL,
  `contactId` int(11) NOT NULL,
  `watch` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`paperId`,`contactId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `ReviewRating`
--


CREATE TABLE `ReviewRating` (
  `paperId` int(11) NOT NULL,
  `reviewId` int(11) NOT NULL,
  `contactId` int(11) NOT NULL,
  `rating` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`paperId`,`reviewId`,`contactId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `ReviewRequest`
--


CREATE TABLE `ReviewRequest` (
  `paperId` int(11) NOT NULL,
  `name` varchar(120) DEFAULT NULL,
  `email` varchar(120) DEFAULT NULL,
  `reason` varbinary(32767) DEFAULT NULL,
  `requestedBy` int(11) NOT NULL,
  `reviewRound` int(1) DEFAULT NULL,
  UNIQUE KEY `paperEmail` (`paperId`,`email`),
  KEY `paperId` (`paperId`),
  KEY `requestedBy` (`requestedBy`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `Settings`
--


CREATE TABLE `Settings` (
  `name` varbinary(256) DEFAULT NULL,
  `value` int(11) NOT NULL,
  `data` varbinary(32767) DEFAULT NULL,
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `TopicArea`
--


CREATE TABLE `TopicArea` (
  `topicId` int(11) NOT NULL AUTO_INCREMENT,
  `topicName` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`topicId`),
  KEY `topicName` (`topicName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



--
-- Table structure for table `TopicInterest`
--


CREATE TABLE `TopicInterest` (
  `contactId` int(11) NOT NULL,
  `topicId` int(11) NOT NULL,
  `interest` int(1) NOT NULL,
  PRIMARY KEY (`contactId`,`topicId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
