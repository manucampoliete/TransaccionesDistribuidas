/* ---------------------------------------------------------------------------------------------- */
/* - Imports ------------------------------------------------------------------------------------ */
/* ---------------------------------------------------------------------------------------------- */
use std::fs::{self, OpenOptions};
use std::io::Write as IoWrite;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use rand::Rng;

/* ---------------------------------------------------------------------------------------------- */
/* - Constants ---------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
const PARTICIPANTS: u32 = 3;
const YES_PROBABILITY: f64 = 0.9;

/* ---------------------------------------------------------------------------------------------- */
/* - Enums + Structs ---------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
#[derive(PartialEq, Debug)]
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

/* ---------------------------------------------------------------------------------------------- */
/* - Write-Ahead Log ------------------------------------------------------------------------------ */
/* ---------------------------------------------------------------------------------------------- */
// Registra la intención en disco ANTES de ejecutar la acción. Si el proceso
// cae justo después de aplicar la acción pero antes de confirmarlo, el log
// ya refleja qué se pretendía hacer y permite recuperar el estado correcto.
fn wal_append(id: u32, entry: &str) {
    let filename = format!("wal_participant_{}.log", id);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let line = format!("[{}] Participant {}: {}\n", timestamp, id, entry);

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&filename)
        .expect("no se pudo abrir el archivo de WAL");
    file.write_all(line.as_bytes())
        .expect("no se pudo escribir en el WAL");

    println!("[Participant {}]: WAL <- {}", id, entry);
}

/* ---------------------------------------------------------------------------------------------- */
/* - Coordinator -------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
fn send_to_all(participants: &[Sender<Message>], build_msg: impl Fn() -> Message) {
    for p in participants {
        p.send(build_msg()).unwrap();
    }
}

fn coordinate_transaction(participants: Vec<Sender<Message>>, rx: Receiver<Message>) {
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
    if all_yes {
        println!("[Coordinator]: all votes YES -> COMMIT");
        send_to_all(&participants, || Message::Commit);
    } else {
        println!("[Coordinator]: at least one NO -> ROLLBACK");
        send_to_all(&participants, || Message::Rollback);
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
                    wal_append(id, "Prepared");
                    println!("[Participant {}]: voting YES", id);
                    coord_tx.send(Message::Vote(Vote::Yes)).unwrap();
                } else {
                    println!("[Participant {}]: voting NO", id);
                    coord_tx.send(Message::Vote(Vote::No)).unwrap();
                }
            }
            Message::Commit => {
                wal_append(id, "Commit");
                println!("[Participant {}]: COMMIT applied", id);
            }
            Message::Rollback => {
                wal_append(id, "Rollback");
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
    // Limpiamos los logs de una corrida anterior para que el archivo refleje
    // solo esta transacción.
    for id in 0..PARTICIPANTS {
        let _ = fs::remove_file(format!("wal_participant_{}.log", id));
    }

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

    drop(tx_to_coord);

    coordinate_transaction(participant_senders, rx_from_participants);

    for handle in handles {
        handle.join().unwrap();
    }
}

/* ---------------------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------------------- */
