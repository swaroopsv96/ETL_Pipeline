# Fiber AI's engineering interview challenges

Welcome! We're excited to be considering you for a full-stack engineering role at Fiber AI. This folder (which will be the basis for a private GitHub repository) contains several engineering challenges for you to complete over the next few days.

## What's inside

You'll find 3 challenges here, each in its own folder:

- `challenge-1` will test your data processing and pipelining skills. You'll process large files with the Node Streams API and convert CSVs into SQL commands with Knex.
- `challenge-2` will test your web scraping abilities. You'll use tools like Crawlee and Cheerio.
- `challenge-3` will test your frontend development skills with React. You'll also use new tools like Chakra UI.

Each challenge is self-contained, although they share some NPM modules (which is why the `package.json` file is out in the parent folder). Feel free to install any other modules that might help you; be sure they're compatible with TypeScript.

You should expect to spend about 6 hours on each challenge.

## Skills needed

Before starting this challenge, you should be familiar with:

- TypeScript
- React
- SQL

Each challenge will introduce new tools and libraries, but you aren't expected to be familiar with them already. They're straightforward enough that you should be able to pick them up on the fly. We use these exact same tools inside Fiber AI, so if you end up joining the team, you'll be well prepared for the job.

## Tooling

The main tools you'll need are Node (version 18+) and NPM. Once you've [installed these](https://nodejs.org/en/download), you should also run this:

```sh
npm install --global tsx
```

`tsx` is a useful tool for running TypeScript files from the command-line, like `node` is for JavaScript files. You'll be using that throughout the challenge.

## Packages

Most of the Node.js modules you'll need for these challenges have already been added to `package.json`, so you'll get them instantly once you do `npm install`. Take these as helpful hints for which tools you should use when tackling these challenges!

We've also pre-installed a handful of useful utility libraries, such as `lodash`, `immutable`, `dayjs`, and `axios`.

Feel free to install other packages if you need them!

## External resources policy

We use Stack Overflow and ChatGPT on the job, so you're allowed to use them as part of this coding challenge. However, **please leave comments indicating which pieces of code you got from Stack Overflow or ChatGPT**, including links to any applicable Stack Overflow discussions. And be sure to double-check the code you get from these sources, because they're often subtly wrong or outdated.

## Submission

You probably got this code as a ZIP file that you unzipped. You should upload this to a new private GitHub repository and start working on it there. Once you're done with the challenge, invite the `fiberai` account as a collaborator on your repository and ping the Fiber team to let us know you're done. We'll then start reviewing your work!

## Questions?

Ping us on Slack, email, or however we got in touch with you if you have any questions about expectations or submission. If you find any ambiguity or missing information in this repository, let us know!
