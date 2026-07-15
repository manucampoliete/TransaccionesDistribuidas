/* ---------------------------------------------------------------------------------------------- */
/* - Imports ------------------------------------------------------------------------------------ */
/* ---------------------------------------------------------------------------------------------- */
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use std::time::Duration;
use rand::Rng;

/* ---------------------------------------------------------------------------------------------- */
/* - Constants ---------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
const PARTICIPANTS: u32 = 3;
const YES_PROBABILITY: f64 = 0.9;

/* ---------------------------------------------------------------------------------------------- */
/* - Enums + Structs ---------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
#[derive(PartialEq, Debug, Clone)]
enum Vote {
    Yes,
    No,
}

#[derive(Clone)]
enum Message {
    Prepare,
    Commit,
    Rollback,
    Vote(Vote),
}

/* ---------------------------------------------------------------------------------------------- */
/* - Coordinator -------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
fn send_to_all(participants: &[Sender<Message>], msg: Message) {
    for p in participants {
        p.send(msg.clone()).unwrap();
    }
}

fn coordinate_transaction(participants: Vec<Sender<Message>>, rx: Receiver<Message>) {
    // Phase 1: Prepare
    println!("[Coordinator]: sending PREPARE to {} participants", participants.len());
    send_to_all(&participants, Message::Prepare);

    // Wait for votes
    let mut votes = Vec::new();
    for _ in 0..participants.len() {
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Message::Vote(v)) => {
                println!("[Coordinator]: received vote {:?}", v);
                votes.push(v);
            }
            _ => {
                println!("[Coordinator]: timeout or unexpected message -> ROLLBACK");
                send_to_all(&participants, Message::Rollback);
                return;
            }
        }
    }

    // Phase 2: Decision
    let all_yes = votes.iter().all(|v| *v == Vote::Yes);
    if all_yes {
        println!("[Coordinator]: all votes YES -> COMMIT");
        send_to_all(&participants, Message::Commit);
    } else {
        println!("[Coordinator]: at least one NO -> ROLLBACK");
        send_to_all(&participants, Message::Rollback);
    }
}

/* ---------------------------------------------------------------------------------------------- */
/* - Participants ------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
fn check_local_conditions(_id: u32) -> bool {
    rand::rng().random::<f64>() < YES_PROBABILITY
}

fn participant_loop(id: u32, coord_tx: Sender<Message>, rx: Receiver<Message>) {
    while let Ok(msg) = rx.recv() {
        match msg {
            Message::Prepare => {
                if check_local_conditions(id) {
                    println!("[Participant {}]: voting YES", id);
                    coord_tx.send(Message::Vote(Vote::Yes)).unwrap();
                } else {
                    println!("[Participant {}]: voting NO", id);
                    coord_tx.send(Message::Vote(Vote::No)).unwrap();
                }
            }
            Message::Commit => {
                println!("[Participant {}]: COMMIT applied", id);
            }
            Message::Rollback => {
                println!("[Participant {}]: ROLLBACK applied", id);
            }
            Message::Vote(_) => unreachable!("un participante no recibe votos"),
        }
    }
}

/* ---------------------------------------------------------------------------------------------- */
/* - Entry Point -------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
fn main() {
    let (tx_to_coord, rx_from_participants) = channel();

    let mut participant_senders = Vec::new();
    let mut handles = Vec::new();

    for id in 0..PARTICIPANTS {
        let (tx_to_participant, rx_from_coord) = channel();
        let coord_tx_clone = tx_to_coord.clone();

        participant_senders.push(tx_to_participant);

        let handle = thread::spawn(move || {
            participant_loop(id, coord_tx_clone, rx_from_coord);
        });
        handles.push(handle);
    }

    // Close original tx (only tx_clone remain in the threads)
    drop(tx_to_coord);

    coordinate_transaction(participant_senders, rx_from_participants);

    for handle in handles {
        handle.join().unwrap();
    }
}

/* ---------------------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */