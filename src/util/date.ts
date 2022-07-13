import { zonedTimeToUtc } from 'date-fns-tz';

const LEGACY_DATE_PATTERN = /^(\d+)\/(\d+)\/(\d+) (\d+):(\d+)(?::(\d+))? (AM|PM)$/i;
const ISO_UTC_DATE_PATTERN = /^\d+-\d+-\d+T\d+:\d+:\d+\.?\d*Z$/;

// Detects legacy dates and converts to UTC dates
//
// Parameters:
//   legacyDate: a string that may or may not be a legacy date
//   tz (optional): an IANA timezone string, default is 'America/New_York'
//
// Returns:
//   null - if the string isn't a legacy date
//   Date - if the string is a legacy date, converted to UTC
export const tryParseLegacyDate = (legacyDate: string, tz: string = 'America/New_York') => {
  const matches = legacyDate.trim().match(LEGACY_DATE_PATTERN);

  if (matches === null) {
    return null;
  }

  // extract capture groups
  let [
    month, day, year,
    hours, minutes, seconds,
  ] = matches.slice(1).map(n => Number(n));
  const ampm = matches[matches.length - 1].toUpperCase();

  // adjust to 24h time
  if (ampm === 'PM') {
    hours += 12;
  }

  // month is zero indexed, so subtract one
  const localDate = new Date(year, month - 1, day);
  // set the rest using functions, because they handle overflow
  localDate.setHours(hours, minutes);
  if (!Number.isNaN(seconds)) {
    localDate.setSeconds(seconds);
  }

  const utcDate = zonedTimeToUtc(localDate, tz);

  //console.log('Found legacy date: ', legacyDate, '-->', utcDate);

  return utcDate;
};

export const isIsoUtcDate = (date: string) => {
  const matches = date.match(ISO_UTC_DATE_PATTERN);

  return !!matches;
};

