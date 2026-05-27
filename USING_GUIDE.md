Keydion Using Guide

This guide walks you through the main ways to find, preview, and download papers on Keydion. It covers the five core mechanisms: the regular search, the advanced search, the Refine Results sidebar, the download options on the preview page, and the information sidebar shown next to the PDF preview.


1. Available Searching Methods

Keydion lets you find papers in several different ways. You can use any of these on their own or combine them through filters.

a. Search by keyword
Type any word or phrase into the search bar at the top of the Search page (or the search box on the landing page). The system looks for matches in the paper title, author name, the keywords field provided by the contributor, the EE subject, the CP global context, and as a fallback it also scans the full text of the PDF itself. This means even if a word does not appear in the metadata, you can still find the paper if it appears inside the document.

b. Search by title
You can either type words from the title into the main keyword box, or use the Advanced Search page to restrict the match to the title field only. Searching by title is useful when you remember part of the name of a paper but not the author.

c. Search by author
On the Advanced Search page there is a dedicated Authors field. Typing a name there filters results so that only papers written by an author whose name contains your text are returned. You can also reach an author search quickly from the preview page by clicking on an author name and then choosing Search this author in the popover.

d. Search by EE subject
When the Paper Type filter is set to Extended Essay, an EE Subject dropdown appears in both the Refine Results sidebar and the Advanced Search page. You can pick any IB Diploma EE subject from the list to narrow results to essays in that subject area.

e. Search by Community Project context
When the Paper Type filter is set to Community Project, a Global Context dropdown appears in the Refine Results sidebar and on the Advanced Search page. You can pick any of the MYP global contexts to narrow results to projects framed by that context.

f. Search by category, language, date, and paper type
The Refine Results sidebar on the search page also exposes filters for Subject Category (the high-level subject grouping), Language (Chinese or English), Date (publication year), and Paper Type (Extended Essay, Community Project, or Independent Research).


2. Advanced Search

Where to access it
The Advanced Search page is reached from the landing page. There is a small Advanced Search link directly under the main search bar in the hero section, and a second link in the footer navigation. You can also navigate to it directly through the URL path used by your deployment.

How it works
The Advanced Search page presents a structured form so you can mix several criteria in one query instead of typing everything into a single keyword box.

The form is organised in three blocks:

Paper Type
This dropdown is at the top because it controls which extra fields appear below. If you choose Extended Essay, the EE Subject dropdown becomes visible and the free-text keywords field is hidden, because EE papers are indexed by structured fields rather than free keywords. If you choose Community Project, the Global Context dropdown becomes visible and the keywords field is hidden in the same way. If you choose Independent Research or leave it as Any Type, the keywords field is shown.

Find articles
This block lets you fill in three independent text fields: terms (matched against keywords and the paper body), authors (matched against the author name), and title (matched against the paper title only). You can fill in any combination of them.

Refine your results by
This block lets you narrow further with the EE Subject or Global Context dropdown (depending on Paper Type) and a publication date range with a start year and an end year.

When you submit the form, you are taken to the regular search results page with all of your criteria applied. From there you can adjust the filters using the Refine Results sidebar without going back.


3. Refine Results Section in search.html

The Refine Results panel runs down the left side of the search results page. It is a live filter form: most controls submit the form automatically as soon as you change them, so you do not need to click a separate Apply button.

Search Within Results
A small input at the top lets you add or change a keyword while keeping all of your other filters intact.

Subject
A dropdown listing the high-level subject categories defined by the site. Choose one to limit results to that subject, or leave it as Any Subject.

Date
A text input where you can type a publication year such as 2023. The filter applies when you click away from the field.

Paper Type
A dropdown with three options: Extended Essay, Community Project, and Independent Research. Selecting one of these reveals an additional dropdown below: choosing Extended Essay reveals EE Subject, and choosing Community Project reveals Global Context.

EE Subject
Visible only when Paper Type is set to Extended Essay. Lists all available IB Diploma EE subjects so you can target the discipline you care about.

Global Context
Visible only when Paper Type is set to Community Project. Lists the MYP global contexts so you can narrow projects by their framing.

Language
A dropdown with Chinese, English, or Any Language. Use this to limit results to papers written in your preferred language.

A back link at the bottom of the panel takes signed-in users to the dashboard and guests to the home page.


4. Downloading on the Preview Page

The preview page is where you land when you click the title or the View button of a result. The download controls live in the top right corner of the page, opposite the Back to Search link.

If you are signed in, you will see two buttons:

Open in new tab
This opens the original PDF file in a new browser tab. It uses the browser's built-in PDF viewer, so you can use the browser's own controls to scroll, zoom, print, or save the file to your computer.

Download
This downloads the PDF directly to your device. The file name is the one the paper was uploaded with.

If you are browsing as a guest (not signed in), these buttons are replaced by a single Sign in to download button that sends you back to the home page with the login dialog open. Guest preview is also limited to the first two pages of the PDF, with a notice shown above the document.

There is also a small download icon on each result card in the search results list, so you can download a paper without opening the preview page at all. Guests see a Sign in to Download button in that same spot.


5. The Sidebar on the Preview Page

The left column of the preview page is a collapsible information sidebar that sits next to the PDF viewer. The sections that appear depend on what kind of paper you are viewing.

IB Extended Essay section
This section appears at the top whenever the paper is an Extended Essay. It is open by default.

It shows the EE subject of the essay, including the interdisciplinary subject if one was registered.

Below that, it lists the five EE assessment criteria: A. Focus and method, B. Knowledge and understanding, C. Critical thinking, D. Presentation, and E. Engagement. Each row shows the criterion letter and label on the left and a coloured score pill on the right showing the score achieved out of the maximum. The pill is green for strong scores, yellow for middle scores, and red for low scores, giving you a visual sense of performance at a glance.

The EE comments part is the most useful element for readers who want to learn from the paper. If the contributor entered an examiner-style comment for a criterion, the row becomes clickable: a small arrow appears on the left, and clicking the row expands a panel below showing the commentary for that criterion. Criteria without a comment stay as plain rows. Below the five criteria you may also see a Holistic Commentary section, which expands to reveal an overall comment on the essay as a whole rather than on any single criterion. Together, the per-criterion comments and the holistic commentary let you understand not only what score the essay received but also why, which is particularly valuable when you are using the paper as a reference for your own work.

MYP Community Project section
This section appears at the top whenever the paper is a Community Project. It is open by default.

At the top it shows a large total score badge out of eight. Below that it lists the Global Context the project was framed by, and the Types of Action the project involved. Then it lists the four MYP project criteria (A through D), each with its own score pill out of the maximum for that criterion.

Abstract section
For papers that are neither EE nor CP, an Abstract section is shown. It is collapsed by default and expands to show the full abstract supplied by the contributor.

About this issue section
This section is available for every paper type. It expands to show the paper's title, category, keywords, and the organisation or school of the authors. It is a quick reference for the structured metadata of the paper.

Related text section
This section is open by default and lists other papers in the same category, with each title linking through to its own preview page. It is a fast way to discover similar work without going back to the search page.

Author popovers
If the paper has multiple authors with parsed details, each author name in the header is clickable. Clicking a name opens a small popover that shows the author's school and email if available, plus a Search this author button that runs an author search for that name.


End of guide.
