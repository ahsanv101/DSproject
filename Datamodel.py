# Generic Classe

class identifiableEntity(object):
    def __init__(self, ids):
        self.id = set()
        for identifier in ids:
            self.id.add(identifier)

    def getIds(self):
        result = []
        for identifier in self.id:
            result.append(identifier)
        result.sort()
        return result


class Person(identifiableEntity):
    def __init__(self, ids, givenName, familyName):
        self.givenName = givenName
        self.familyName = familyName
        super().__init__(ids)

    def getGivenName(self):
        return self.givenName

    def getFamilyName(self):
        return self.familyName

    # Publications


class Publication(identifiableEntity):
    def __init__(self, ids, publicationYear, title, publicationVenue, cites, author):
        self.publicationYear = publicationYear
        self.title = title
        self.publicationVenue = publicationVenue
        self.author = set()
        for name in author:
            self.author.add(name)
        self.cites = set()
        for cite in cites:
            self.cites.add(cite)

        super().__init__(ids)

    def getPublicationYear(self):
        return self.publicationYear

    def getTitle(self):
        return self.title

    def getPublicationVenue(self):
        return self.publicationVenue

    def getAuthors(self):
        return self.author

    def getCitedPublications(self):
        result = []
        for cite in self.cites:
            result.append(cite)
        return result


class JournalArticle(Publication):
    def __init__(self, ids, publicationYear, title, publicationVenue, cites, author, issue, volume):
        self.issue = issue
        self.volume = volume

        super().__init__(ids, publicationYear, title, publicationVenue, cites, author)

    def getIssue(self):
        return self.issue

    def getVolume(self):
        return self.volume


class BookChapter(Publication):
    def __init__(self, ids, publicationYear, title, publicationVenue, cites, author, chapterNumber):
        self.chapterNumber = chapterNumber
        super().__init__(ids, publicationYear, title, publicationVenue, cites, author)

    def getchapterNumber(self):
        return self.chapterNumber


class ProceedingsPaper(Publication):
    def __init__(self, ids, publicationYear, title, publicationVenue, cites, author):
        super().__init__(ids, publicationYear, title, publicationVenue, cites, author)


# Venue

class Organization(identifiableEntity):
    def __init__(self, ids, name):
        self.name = name
        super().__init__(ids)

    def getName(self):
        return self.name


class Venue(identifiableEntity):
    def __init__(self, ids, title, publisher):
        self.title = title
        self.publisher = publisher

        super().__init__(ids)

    def getTitle(self):
        return self.title

    def getPublisher(self):
        return self.publisher


class Proceedings(Venue):
    def __init__(self, ids, title, publisher, event):
        self.event = event
        super().__init__(ids, title, publisher)

    def getEvent(self):
        return self.event


class Book(Venue):
    def __init__(self, ids, title, publisher):
        super().__init__(ids, title, publisher)


class Journal(Venue):
    def __init__(self, ids, title, publisher):
        super().__init__(ids, title, publisher)

