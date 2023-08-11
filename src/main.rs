use std::collections::HashMap;

/// A program is just a list of identities. The polynomials are implicit. We do not differenciate between constant and committed
#[derive(Default, Debug)]
struct Program {
    identities: Vec<Identity<Column>>,
}

/// We support `a = b` and `a in b`
#[derive(Debug)]
enum Identity<C> {
    Pol(Shifted<C>, Shifted<C>),
    Lookup(Shifted<C>, Shifted<C>),
}

#[derive(Hash, Eq, PartialEq, Debug)]
struct Column {
    name: String,
}

#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy)]
struct IndexedColumn<'a> {
    name: &'a str,
    index: usize,
}

/// A shifted column, just an extra flag, used in identities
#[derive(Default, Debug, Clone, Copy)]
pub struct Shifted<C> {
    column: C,
    shifted: bool,
}

impl<C> From<C> for Shifted<C> {
    fn from(c: C) -> Self {
        Self {
            column: c,
            shifted: false,
        }
    }
}

impl<S> Shifted<S> {
    fn next(mut self) -> Self {
        self.shifted = true;
        self
    }
}

/// A set of machines, detected from a program
#[derive(Default, Debug)]
struct Machines<'a> {
    v: Vec<Machine<'a>>,
}

/// A detected machine
#[derive(Default, Debug)]
struct Machine<'a> {
    /// these are the connections to other machines. Basically lookups become this
    links: Vec<Link<'a>>,
    /// the columns for this machine
    columns: Vec<IndexedColumn<'a>>,
    /// the identities *internal to this machine*. the rest are in the links
    identities: Vec<Identity<IndexedColumn<'a>>>,
}

/// the links to other machines
#[derive(Debug)]
struct Link<'a> {
    /// the index of the target machine in the machine list
    to: usize,
    /// which local column should equal which target column
    columns: (Shifted<IndexedColumn<'a>>, Shifted<IndexedColumn<'a>>),
}

impl<'a> From<&'a Program> for Machines<'a> {
    /// detection of machines in a program. it's buggy but it works on my example lol
    /// also it can be ignored as this is not what this prototype is about
    fn from(p: &'a Program) -> Self {
        // detect fully connected with pol
        // link with lookups

        p.identities
            .iter()
            .fold(
                (HashMap::<&'a Column, usize>::default(), Machines::default()),
                |(mut column_to_machine, mut machines), identity| {
                    match identity {
                        Identity::Pol(left, right) => {
                            let lid = column_to_machine.get(&left.column);
                            let rid = column_to_machine.get(&right.column);
                            match (lid, rid) {
                                (Some(lid), Some(rid)) => {
                                    assert_eq!(lid, rid);
                                    let machine = &mut machines.v[*lid];
                                    let left_column = IndexedColumn {
                                        name: &left.column.name,
                                        index: *lid,
                                    };
                                    let right_column = IndexedColumn {
                                        name: &right.column.name,
                                        index: *rid,
                                    };
                                    let left_shifted = Shifted {
                                        column: left_column,
                                        shifted: left.shifted,
                                    };
                                    let right_shifted = Shifted {
                                        column: right_column,
                                        shifted: right.shifted,
                                    };

                                    machine
                                        .identities
                                        .push(Identity::Pol(left_shifted, right_shifted));
                                }
                                (Some(lid), None) => {
                                    let machine = &mut machines.v[*lid];
                                    let c_id = machine.columns.len();
                                    let left_column = IndexedColumn {
                                        name: &left.column.name,
                                        index: *lid,
                                    };
                                    let right_column = IndexedColumn {
                                        name: &right.column.name,
                                        index: c_id,
                                    };
                                    column_to_machine.insert(&right.column, *lid);
                                    machine.columns.push(right_column);

                                    let left_shifted = Shifted {
                                        column: left_column,
                                        shifted: left.shifted,
                                    };
                                    let right_shifted = Shifted {
                                        column: right_column,
                                        shifted: right.shifted,
                                    };

                                    machine
                                        .identities
                                        .push(Identity::Pol(left_shifted, right_shifted));
                                }
                                (None, Some(rid)) => {
                                    let machine = &mut machines.v[*rid];
                                    let c_id = machine.columns.len();
                                    let left_column = IndexedColumn {
                                        name: &left.column.name,
                                        index: c_id,
                                    };
                                    let right_column = IndexedColumn {
                                        name: &right.column.name,
                                        index: *rid,
                                    };
                                    column_to_machine.insert(&left.column, *rid);
                                    machine.columns.push(left_column);

                                    let left_shifted = Shifted {
                                        column: left_column,
                                        shifted: left.shifted,
                                    };
                                    let right_shifted = Shifted {
                                        column: right_column,
                                        shifted: right.shifted,
                                    };

                                    machine
                                        .identities
                                        .push(Identity::Pol(left_shifted, right_shifted));
                                }
                                (None, None) => {
                                    let mut machine = Machine::default();

                                    let left_id = machine.columns.len();
                                    let left_column = Shifted {
                                        column: IndexedColumn {
                                            name: &left.column.name,
                                            index: left_id,
                                        },
                                        shifted: left.shifted,
                                    };
                                    machine.columns.push(left_column.column);

                                    let right_id = machine.columns.len();
                                    let right_column = Shifted {
                                        column: IndexedColumn {
                                            name: &right.column.name,
                                            index: right_id,
                                        },
                                        shifted: right.shifted,
                                    };
                                    machine.columns.push(right_column.column);

                                    machine
                                        .identities
                                        .push(Identity::Pol(left_column, right_column));
                                    machines.v.push(machine);
                                    let m_id = machines.v.len() - 1;
                                    column_to_machine.insert(&left.column, m_id);
                                    column_to_machine.insert(&right.column, m_id);
                                }
                            }
                        }
                        Identity::Lookup(from, to) => {
                            let fromid = column_to_machine.get(&from.column).cloned();
                            let toid = column_to_machine.get(&to.column).cloned();
                            match (fromid, toid) {
                                (Some(fromid), Some(toid)) => {
                                    if fromid == toid {
                                        machines.v[fromid].identities.push(Identity::Lookup(
                                            Shifted {
                                                column: IndexedColumn {
                                                    name: &from.column.name,
                                                    index: fromid,
                                                },
                                                shifted: from.shifted,
                                            },
                                            Shifted {
                                                column: IndexedColumn {
                                                    name: &to.column.name,
                                                    index: toid,
                                                },
                                                shifted: to.shifted,
                                            },
                                        ));
                                    } else {
                                        machines.v[fromid].links.push(Link {
                                            to: toid,
                                            columns: (
                                                Shifted {
                                                    column: IndexedColumn {
                                                        name: &from.column.name,
                                                        index: fromid,
                                                    },
                                                    shifted: from.shifted,
                                                },
                                                Shifted {
                                                    column: IndexedColumn {
                                                        name: &to.column.name,
                                                        index: toid,
                                                    },
                                                    shifted: to.shifted,
                                                },
                                            ),
                                        });
                                    }
                                }
                                (Some(fromid), None) => {
                                    let mut machine = Machine::default();
                                    let column_id = machine.columns.len();
                                    machine.columns.push(IndexedColumn {
                                        name: &to.column.name,
                                        index: column_id,
                                    });
                                    machines.v.push(machine);
                                    let m_id = machines.v.len() - 1;
                                    column_to_machine.insert(&to.column, m_id);
                                    machines.v[fromid].links.push(Link {
                                        to: m_id,
                                        columns: (
                                            Shifted {
                                                column: IndexedColumn {
                                                    name: &from.column.name,
                                                    index: fromid,
                                                },
                                                shifted: from.shifted,
                                            },
                                            Shifted {
                                                column: IndexedColumn {
                                                    name: &to.column.name,
                                                    index: column_id,
                                                },
                                                shifted: to.shifted,
                                            },
                                        ),
                                    });
                                }
                                (None, Some(toid)) => {
                                    let mut machine = Machine::default();
                                    let column_id = machine.columns.len();
                                    machine.columns.push(IndexedColumn {
                                        name: &from.column.name,
                                        index: column_id,
                                    });
                                    machines.v.push(machine);
                                    let m_id = machines.v.len() - 1;
                                    column_to_machine.insert(&from.column, m_id);
                                    machines.v[m_id].links.push(Link {
                                        to: toid,
                                        columns: (
                                            Shifted {
                                                column: IndexedColumn {
                                                    name: &from.column.name,
                                                    index: column_id,
                                                },
                                                shifted: from.shifted,
                                            },
                                            Shifted {
                                                column: IndexedColumn {
                                                    name: &to.column.name,
                                                    index: toid,
                                                },
                                                shifted: to.shifted,
                                            },
                                        ),
                                    });
                                }
                                (None, None) => {
                                    unimplemented!()
                                }
                            }
                        }
                    }
                    (column_to_machine, machines)
                },
            )
            .1
    }
}

/// An allocator for rows of a given width
#[derive(Debug)]
struct Allocator {
    data: Vec<Vec<Option<usize>>>,
    width: usize,
}

impl Allocator {
    /// we create one of these for each machine instance
    fn with_width(width: usize) -> Self {
        Self {
            data: Default::default(),
            width,
        }
    }

    /// we can call it to add a row, but we could also allocate all the rows at first
    fn add_row(&mut self) {
        self.data.push(vec![None; self.width])
    }

    /// we can create a writer over the underlying data of this allocator, targetting a given row
    fn writer(&mut self, row: usize) -> Writer {
        Writer {
            row,
            data: &mut self.data,
        }
    }
}

/// a transient writer to the data of an instance. Not sure if it's strictly required to separate it.
struct Writer<'a> {
    /// the row this writer targets in the data
    row: usize,
    data: &'a mut Vec<Vec<Option<usize>>>,
}

impl<'a> Writer<'a> {
    /// if we're at row 42 writing `a'`, we want to target row 43. this method gets the correct index.
    fn row<C>(&self, c: &Shifted<C>) -> usize {
        self.row + c.shifted.then_some(1).unwrap_or_default()
    }

    /// set some cell
    fn set(&mut self, i: &Shifted<IndexedColumn<'a>>, v: usize) {
        let row = self.row(i);
        self.data[row][i.column.index] = Some(v);
    }

    /// get some cell
    fn get(&self, i: &Shifted<IndexedColumn<'_>>) -> Option<usize> {
        self.data[self.row(i)][i.column.index]
    }
}

/// A processor attaches to a machine to process it
#[derive(Debug)]
struct Processor<'a> {
    /// the row index we're at
    row: usize,
    /// the witness data
    allocator: Allocator,
    /// the machine type we're processing
    machine: Machine<'a>,
}

/// A solver takes data and an identity at a given row and learns new values
/// This is the bit which could differ between block machines and vms
#[derive(Debug, Default)]
struct Solver {
    row: usize,
}

impl Solver {
    fn index<C>(&self, c: &Shifted<C>) -> usize {
        self.row + c.shifted.then_some(1).unwrap_or_default()
    }

    fn process_identity<'a>(
        &self,
        identity: &Identity<IndexedColumn<'a>>,
        data: &[Vec<Option<usize>>],
    ) -> Vec<(Shifted<IndexedColumn<'a>>, usize)> {
        match identity {
            Identity::Pol(left, right) => {
                match (
                    data[self.index(left)]
                        .get(left.column.index)
                        .as_ref()
                        .unwrap(),
                    data[self.index(right)]
                        .get(right.column.index)
                        .as_ref()
                        .unwrap(),
                ) {
                    (Some(left), Some(right)) => {
                        assert_eq!(left, right);
                        vec![]
                    }
                    (Some(left), None) => {
                        vec![(*right, *left)]
                    }
                    (None, Some(right)) => {
                        vec![(*left, *right)]
                    }
                    (None, None) => vec![],
                }
            }
            _ => unimplemented!("internal lookups unimp"),
        }
    }
}

impl<'a> Processor<'a> {
    fn process_identities(&mut self) {
        // propagate using the identities
        for identity in &self.machine.identities {
            for (key, value) in self
                .solver()
                .process_identity(identity, &self.allocator.data)
            {
                self.allocator.writer(self.row).set(&key, value);
            }
        }
    }

    fn solver(&self) -> Solver {
        Solver { row: self.row }
    }

    /// right now the row is incremented after we're done with a row
    /// but this could be delegated to the solver, which knows where to look next, and could go backwards etc
    fn increment_row(&mut self) {
        self.row += 1;
        self.allocator.add_row();
    }
}

/// We create a processor for a given machine by attaching an allocator and starting at row 0
impl<'a> From<Machine<'a>> for Processor<'a> {
    fn from(machine: Machine<'a>) -> Self {
        Self {
            /// we start with two rows, so that on the first row we can write to 0 and 1, etc
            allocator: {
                let mut a = Allocator::with_width(machine.columns.len());
                a.add_row();
                a.add_row();
                a
            },
            machine,
            row: 0,
        }
    }
}

/// all the processors together!
#[derive(Debug)]
struct Processors<'a> {
    v: Vec<Processor<'a>>,
}

impl<'a> Processors<'a> {
    /// process the processor at index `p_id` for `length` rows, initialising its column with `values`
    /// this is the entry point
    fn process_main(
        mut self,
        p_id: usize,
        // this gets turned into the shifted indexed versions
        values: Vec<(Column, usize)>,
        length: usize,
    ) -> Vec<Vec<Vec<Option<usize>>>> {
        // we've allocated two rows when building the processors so we can safely put the first values
        let mut values = values
            .iter()
            .map(|(column, value)| {
                (
                    (*self.v[p_id]
                        .machine
                        .columns
                        .iter()
                        .find(|indexed| indexed.name == column.name)
                        .unwrap())
                    .into(),
                    *value,
                )
            })
            .collect();

        // process `length` rows of this machine!
        for _ in 0..length {
            self.process(p_id, values);
            // we remove this so that this only applies the first time...
            values = vec![];
        }

        // we dump the data for all machines
        self.v.into_iter().map(|p| p.allocator.data).collect()
    }

    /// process a processor with some initial values (passed from the outside or through links from other processors)
    /// this is the recursive function!
    fn process(&mut self, p_id: usize, values: Vec<(Shifted<IndexedColumn<'a>>, usize)>) {
        // get the processor
        let processor = &mut self.v[p_id];
        // set the given values
        for (i, v) in values {
            processor.allocator.writer(processor.row).set(&i, v);
        }

        // process all the local identities. this is not a fixed point approach and should probably be one!
        processor.process_identities();

        // generate values for the links
        let calls = processor
            .machine
            .links
            .iter()
            .filter_map(|link| {
                // for each link, get the values from the allocator

                let (from, to) = link.columns;
                processor
                    .allocator
                    // yes, the writer also reads.........
                    .writer(processor.row)
                    .get(&from)
                    // if we know the value
                    .map(|v| vec![(to, v)])
                    .map(|values| (link.to, values))
            })
            .collect::<Vec<_>>();

        // we're done with this row (well probably not but this works on my example)
        processor.increment_row();

        // this is where we recurse: we call the other processors with the values we found in this one
        for (p_id, values) in calls {
            self.process(p_id, values);
        }
    }
}

/// just instanciating the processors
impl<'a> From<Machines<'a>> for Processors<'a> {
    fn from(machines: Machines<'a>) -> Self {
        Self {
            v: machines.v.into_iter().map(From::from).collect(),
        }
    }
}

/// util
fn col(name: &str) -> Shifted<Column> {
    Column {
        name: name.to_string(),
    }
    .into()
}

fn main() {
    let program = Program {
        identities: vec![
            Identity::Pol(col("a"), col("b")),        // a = b
            Identity::Pol(col("a").next(), col("a")), // a' = a
            Identity::Lookup(col("a"), col("d")),     // { a } in { d }
        ],
    };

    // detect the machines
    let machines = Machines::from(&program);

    // initialise the processors
    let processors = Processors::from(machines);

    // execute starting with a = 3 for 10 rows
    let res = processors.process_main(0, vec![(col("a").column, 3)], 10);

    // this fails because we don't quite make it to the end but you can see there are some values in there
    assert_eq!(
        res,
        vec![vec![vec![Some(3), Some(3)]; 10], vec![vec![Some(3)]; 10]]
    );
}
