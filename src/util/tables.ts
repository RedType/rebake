export default (pk: string, sk: string) => {
  const pkPrefix = pk.split('#')[0];
  const skSplit  = sk.split('#');
  const skPrefix = skSplit[0];
  const skSuffix = skSplit[skSplit.length - 1];
  const skType   = skSplit[1];
  const jst = JSON.stringify;

  //console.debug(`--> Coords retrieved: [${pkPrefix}, ${skPrefix}, ${skSuffix}]`);

  switch (jst([pkPrefix, skPrefix])) {
    case jst(['patient', 'appointment']):
      return 'appointments';

    case jst(['patient', 'assessment']):
      if (skSuffix === 'definition') {
        //console.info('Omitting assessment definition record');
        return null; // omit this record
      }

      if (skSuffix === 'inFlight') {
        //console.info('Omitting inFlight assessment record');
        return null; // omit this record
      }

      if (skSuffix === 'result')
        return `assessment_${skType}_results`;

      // else
      //console.warn(`Omitting unclassified record { pk: ${pk}, sk: ${sk} }`);
      return null; // omit this record

    case jst(['patient', 'journey']):
      return 'journeys';

    case jst(['userProfile', 'patientGoalsDef']):
      return 'patientgoalsdefs';

    case jst(['userProfile', 'patient']):
      return 'patients';

    case jst(['userProfile', 'userProfile']):
      return 'userprofiles';

    case jst(['ruleCollection', 'ruleCollection']):
      switch (skSuffix) {
        case 'state':
          return 'rulecollection_state';

        default:
          //console.warn(`Omitting unclassified record { pk: ${pk}, sk: ${sk} }`);
          return null; // omit
      }

    default:
      //console.warn(`Omitting unclassified record { pk: ${pk}, sk: ${sk} }`);
      return null; // omit this record
  }
};

