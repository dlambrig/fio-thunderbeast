// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
//
// rbd engine
//
// IO engine using Ceph's librbd to test RADOS Block Devices.

#include <cstring>

#include <librados/RadosClient.h>
#include <librbd/Image.h>

using std::string;
using librados::RadosClient;
using librbd::Image;

extern "C" {
#undef __le16_to_cpu
#undef __le32_to_cpu
#undef __le64_to_cpu
#undef __cpu_to_le16
#undef __cpu_to_le32
#undef __cpu_to_le64
#undef ARRAY_SIZE
#include "../fio.h"
};

using librados::RadosClient;
using librbd::Image;
using std::memset;
using std::reference_wrapper;
using ceph::bufferlist;
using ceph::bufferptr;

struct cbd_data {
  RadosClient* rados;
  Image* image;
  struct io_u** aio_events;

  cbd_data(size_t iodepth) : rados(nullptr), image(nullptr),
			     aio_events(new io_u*[iodepth]) {
    memset(aio_events, 0, sizeof(io_u*) * iodepth);
  }
  ~cbd_data() {
    if (rados) {
      rados->shutdown();
      delete rados;
    }
    delete image;
    delete aio_events;
  }
};

struct cbd_options {
  struct thread_data *td;
  char *image;
  char *volume;
  char *client;
};

static fio_option options[4];

bool is_complete(void* v) {
  return reinterpret_cast<uintptr_t>(v) & 0x01;
}

void set_complete(void*& v) {
  v = reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(v) | 0x01);
}

void clear_complete(void*& v) {
  v = reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(v) & ~0x01);
}

bool has_read(void* v) {
  return reinterpret_cast<uintptr_t>(v) & 0x02;
}

void set_read(void*& v) {
  v = reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(v) | 0x02);
}

void clear_read(void*& v) {
  v = reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(v) & ~0x02);
}

// This class has no data members and no virtual anything. Thus, we
// are guaranteed by the C++ standard that it is StandardLayout. And,
// in fact, the same StandardLayout as its parent class. This means
// that we can use it as our callback and not have to do any more
// allocations than fio already did. We don't even need a vtable.
//
// Ain't language lawyering wonderful?

struct cbd_io_u : public io_u {
  reference_wrapper<cbd_io_u> static ref(io_u* io) {
    cbd_io_u* _io = static_cast<cbd_io_u*>(io);
    auto r = std::ref(*_io);
    return r;
  }
  cbd_io_u() = delete;
  cbd_io_u(const cbd_io_u&) = delete;
  cbd_io_u(cbd_io_u&&) = delete;

  // Write callback
  void operator()(int r) {
    if (has_read(engine_data)) {
      bufferlist* _bl = reinterpret_cast<bufferlist*>(&(bl));
      _bl->~list();
      clear_read(engine_data);
    }
    set_complete(engine_data);
    assert(is_complete(engine_data));
    error = -r;
  }
};

static struct io_u *fio_cbd_event(struct thread_data *td, int event)
{
  cbd_data* data = static_cast<cbd_data*>(td->io_ops->data);

  return data->aio_events[event];
}

static int fio_cbd_getevents(struct thread_data *td,
			     unsigned int min,
			     unsigned int max,
			     struct timespec *t)
{
  cbd_data* data = static_cast<cbd_data*>(td->io_ops->data);
  size_t events = 0;
  io_u *io = 0;
  int i = 0;

  do {
    io_u_qiter(&td->io_u_all, io, i) {
      if (!(io->flags & IO_U_F_FLIGHT))
	continue;

      if (is_complete(io->engine_data)) {
	io->engine_data = 0;
	data->aio_events[events] = io;
	events++;
      }
    }
    if (events < min) {
      std::this_thread::sleep_for(100us);
    } else {
      break;
    }
  } while (1);

  return events;
}

const char* dirstring(int dir) {
  switch (dir) {
  case DDIR_WRITE:
    return "write";
    break;

  case DDIR_READ:
    return "read";
    break;

  case DDIR_SYNC:
    return "sync";
    break;
  }
}

static int fio_cbd_queue(struct thread_data *td, struct io_u *io)
{
  int code = 0;
  cbd_data* data = static_cast<cbd_data*>(td->io_ops->data);

  fio_ro_check(td, io);

  try {
    if (io->ddir == DDIR_WRITE) {
      bufferlist bl;
      bufferptr bp = buffer::create_static(io->xfer_buflen,
					   static_cast<char*>(io->xfer_buf));
      bl.push_back(bp);
      data->image->write(io->offset, io->xfer_buflen, bl, cbd_io_u::ref(io));
      code = FIO_Q_QUEUED;
    } else if (io->ddir == DDIR_READ) {
      bufferlist* bl = new (&(io->bl)) bufferlist;
      bufferptr bp = buffer::create_static(io->xfer_buflen,
					   static_cast<char*>(io->xfer_buf));
      bl->push_back(bp);
      data->image->read(io->offset, io->xfer_buflen, bl, cbd_io_u::ref(io));
      set_read(io->engine_data);
      code = FIO_Q_QUEUED;
    } else if (io->ddir == DDIR_SYNC) {
      data->image->flush(cbd_io_u::ref(io));
      code = FIO_Q_QUEUED;
    } else {
      dprint(FD_IO, "%s: Warning: unhandled ddir: %d\n", __func__,
	     io->ddir);
      code = FIO_Q_COMPLETED;
    }
  } catch (std::error_condition& e) {
    log_err("Queueing %s operation failed: %s.\n",
	    dirstring(io->ddir), e.message().c_str());
    io->error = e.value();
    code = FIO_Q_COMPLETED;
  }

  return code;
}

static void fio_cbd_cleanup(struct thread_data *td)
{
  cbd_data* data = static_cast<cbd_data*>(td->io_ops->data);
  delete data;
}

static int fio_cbd_setup(struct thread_data *td)
{
  cbd_data* data = nullptr;
  cbd_options *o = static_cast<cbd_options*>(td->eo);
  int r = 0;
  fio_file *f;

  if (td->io_ops->data)
    return 0;

  /* LibRBD supports multiple threads but not multiple forked
     processes*/
  td->o.use_thread = 1;

  try {
    data = new cbd_data(td->o.iodepth);
  } catch (std::bad_alloc &e) {
    // We might have failed allocating the io_u array
    if (data)
      delete data;
    log_err("allocating CBD data failed.\n");
    return 1;
  }

  data->rados = new RadosClient;
  r = data->rados->connect();
  if (r < 0) {
    log_err("Connect to cluster failed.\n");
    delete data;
    return 1;
  }

  VolumeRef v = data->rados->lookup_volume(o->volume);
  if (!v) {
    log_err("Unable to find volume.\n");
    delete data;
    return 1;
  }

  uint64_t image_size = 0;

  try {
    data->image = new Image(data->rados, v, o->image);
    image_size = data->image->get_size();
  } catch (std::error_condition &e) {
    log_err("Unable to open image: %s.\n", e.message().c_str());
    delete data;
    return 1;
  }

  dprint(FD_IO, "cbd-engine: image size: %lu\n", image_size);

  /* taken from "net" engine. Pretend we deal with files, even if we
     do not have any ideas about files.  The size of the RBD is set
     instead of a artificial file. */
  if (!td->files_index) {
    add_file(td, td->o.filename ? : "cbd", 0, 0);
    td->o.nr_files = td->o.nr_files ? : 1;
    td->o.open_files++;
  }
  f = td->files[0];
  f->real_file_size = image_size;

  td->io_ops->data = static_cast<void*>(data);

  return 0;
}

static int fio_cbd_open(struct thread_data *td, struct fio_file *f)
{
  return 0;
}

static void fio_cbd_io_u_free(struct thread_data *td, struct io_u *io_u)
{
  io_u->engine_data = 0;
}

static int fio_cbd_io_u_init(struct thread_data *td, struct io_u *io_u)
{
  io_u->engine_data = 0;
  return 0;
}

static struct ioengine_ops ioengine;

static void fio_init fio_cbd_register(void)
{
  options[0].name = "image";
  options[0].lname = "cbd engine image";
  options[0].type = FIO_OPT_STR_STORE;
  options[0].help = "Image name for CBD engine";
  options[0].off1 = offsetof(struct cbd_options, image);
  options[0].category = FIO_OPT_C_ENGINE;
  options[0].group = FIO_OPT_G_CBD;

  options[1].name = "volume";
  options[1].lname = "cbd engine volume";
  options[1].type = FIO_OPT_STR_STORE;
  options[1].help = "Name of the volume hosting the image for the RBD engine";
  options[1].off1 = offsetof(struct cbd_options, volume);
  options[1].category = FIO_OPT_C_ENGINE;
  options[1].group = FIO_OPT_G_CBD;

  options[2].name = "clientname";
  options[2].lname = "cbd engine clientname";
  options[2].type = FIO_OPT_STR_STORE;
  options[2].help = "Name of the Cohort client for the RBD engine";
  options[2].off1 = offsetof(struct cbd_options, client);
  options[2].category = FIO_OPT_C_ENGINE;
  options[2].group = FIO_OPT_G_CBD;

  options[3].name = NULL;

  strcpy(ioengine.name, "cbd");
  ioengine.version = FIO_IOOPS_VERSION;
  ioengine.setup = fio_cbd_setup;
  ioengine.queue = fio_cbd_queue;
  ioengine.getevents = fio_cbd_getevents;
  ioengine.event = fio_cbd_event;
  ioengine.cleanup = fio_cbd_cleanup;
  ioengine.open_file = fio_cbd_open;
  ioengine.options = options;
  ioengine.io_u_init = fio_cbd_io_u_init;
  ioengine.io_u_free = fio_cbd_io_u_free;
  ioengine.option_struct_size = sizeof(struct cbd_options);

  register_ioengine(&ioengine);
}

static void fio_exit fio_cbd_unregister(void)
{
  unregister_ioengine(&ioengine);
}
