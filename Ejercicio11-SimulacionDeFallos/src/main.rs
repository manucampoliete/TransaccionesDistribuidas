/* ---------------------------------------------------------------------------------------------- */
/* - Imports ------------------------------------------------------------------------------------ */
/* ---------------------------------------------------------------------------------------------- */
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use std::time::Duration;

/* ---------------------------------------------------------------------------------------------- */
/* - Enums + Structs ---------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
#[derive(PartialEq, Debug, Clone, Copy)]
enum Vote {
    Yes,
    No,
}

enum Message {
    Prepare,
    Commit,
    Rollback,
    Vote(Vote),
}

// Comportamiento fijo del participante, para forzar el escenario de falla
// en lugar de dejarlo librado al azar como en el Ejercicio 10.
#[derive(Clone, Copy)]
enum Behavior {
    RespondWith(Vote),
    Delay(Duration, Vote),
}

/* ---------------------------------------------------------------------------------------------- */
/* - Coordinator -------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
fn send_to_all(participants: &[Sender<Message>], build_msg: impl Fn() -> Message) {
    for p in participants {
        p.send(build_msg()).unwrap();
    }
}

fn coordinate_transaction(
    participants: Vec<Sender<Message>>,
    rx: Receiver<Message>,
    force_rollback: bool,
) {
    println!("[Coordinator]: sending PREPARE to {} participants", participants.len());
    send_to_all(&participants, || Message::Prepare);

    let mut votes = Vec::new();
    for _ in 0..participants.len() {
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Message::Vote(v)) => {
                println!("[Coordinator]: received vote {:?}", v);
                votes.push(v);
            }
            _ => {
                println!("[Coordinator]: timeout or unexpected message -> ROLLBACK");
                send_to_all(&participants, || Message::Rollback);
                return;
            }
        }
    }

    let all_yes = votes.iter().all(|v| *v == Vote::Yes);
    if !all_yes {
        println!("[Coordinator]: at least one NO -> ROLLBACK");
        send_to_all(&participants, || Message::Rollback);
    } else if force_rollback {
        println!("[Coordinator]: all votes YES but ROLLBACK forced by coordinator -> ROLLBACK");
        send_to_all(&participants, || Message::Rollback);
    } else {
        println!("[Coordinator]: all votes YES -> COMMIT");
        send_to_all(&participants, || Message::Commit);
    }
}

/* ---------------------------------------------------------------------------------------------- */
/* - Participants ------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
fn participant_loop(id: u32, behavior: Behavior, coord_tx: Sender<Message>, rx: Receiver<Message>) {
    while let Ok(msg) = rx.recv() {
        match msg {
            Message::Prepare => match behavior {
                Behavior::RespondWith(vote) => {
                    println!("[Participant {}]: voting {:?}", id, vote);
                    coord_tx.send(Message::Vote(vote)).unwrap();
                }
                Behavior::Delay(delay, vote) => {
                    println!("[Participant {}]: delaying {:?} before responding", id, delay);
                    thread::sleep(delay);
                    println!("[Participant {}]: voting {:?} (after delay)", id, vote);
                    // El coordinador ya puede haber abortado por timeout; si el receiver
                    // se cerró, el envío falla y lo ignoramos.
                    let _ = coord_tx.send(Message::Vote(vote));
                }
            },
            Message::Commit => println!("[Participant {}]: COMMIT applied", id),
            Message::Rollback => println!("[Participant {}]: ROLLBACK applied", id),
            Message::Vote(_) => unreachable!("un participante no recibe votos"),
        }
    }
}

fn run_transaction(label: &str, behaviors: Vec<Behavior>, force_rollback: bool) {
    println!("\n=== {} ===", label);

    let (tx_to_coord, rx_from_participants) = channel();
    let mut participant_senders = Vec::new();
    let mut handles = Vec::new();

    for (id, behavior) in behaviors.into_iter().enumerate() {
        let (tx_to_participant, rx_from_coord) = channel();
        let coord_tx_clone = tx_to_coord.clone();
        participant_senders.push(tx_to_participant);

        let handle = thread::spawn(move || {
            participant_loop(id as u32, behavior, coord_tx_clone, rx_from_coord);
        });
        handles.push(handle);
    }

    drop(tx_to_coord);
    coordinate_transaction(participant_senders, rx_from_participants, force_rollback);

    for handle in handles {
        handle.join().unwrap();
    }
}

/* ---------------------------------------------------------------------------------------------- */
/* - Entry Point -------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
fn main() {
    // a) Un participante responde NO -> el coordinador debe abortar para todos.
    run_transaction(
        "Caso A: un participante responde NO",
        vec![
            Behavior::RespondWith(Vote::Yes),
            Behavior::RespondWith(Vote::No),
            Behavior::RespondWith(Vote::Yes),
        ],
        false,
    );

    // b) Un participante demora más de 5 segundos -> dispara el timeout del coordinador.
    run_transaction(
        "Caso B: un participante demora más de 5 segundos",
        vec![
            Behavior::RespondWith(Vote::Yes),
            Behavior::Delay(Duration::from_secs(6), Vote::Yes),
            Behavior::RespondWith(Vote::Yes),
        ],
        false,
    );

    // c) Todos votan YES pero el coordinador fuerza Rollback igual (decisión de negocio).
    run_transaction(
        "Caso C: el coordinador fuerza ROLLBACK con votos unánimes",
        vec![
            Behavior::RespondWith(Vote::Yes),
            Behavior::RespondWith(Vote::Yes),
            Behavior::RespondWith(Vote::Yes),
        ],
        true,
    );
}

/* ---------------------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
