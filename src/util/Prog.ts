export default class Prog {
  private count: number;
  private i: number;
  private intervalId: any;

  constructor() {
    this.count = 0;
    this.i = 0;

    process.stdout.write('\n'); // advance to new line
    this.intervalId = setInterval(() => {
      const dots = Array(this.i++ % 3 + 1).map(() => '.').join();
      process.stdout.clearLine(0);
      process.stdout.write(`Processed ${this.count} rows${dots}`);
    }, 1000);
  }

  inc() { ++this.count; }

  stop() {
    clearInterval(this.intervalId);
    process.stdout.clearLine(0);
    process.stdout.write(`Processed ${this.count} rows.\n`);
  }
};

